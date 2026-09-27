package gda

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEgressCost(t *testing.T) {
	p := DefaultPrices()
	for _, test := range []struct {
		path string
		gb   float64
		want float64
	}{
		{EgressInternet, 0, 0},
		{EgressInternet, 100, 9},
		{EgressInternet, 10 * 1024, 10 * 1024 * 0.09},
		{EgressInternet, 20 * 1024, 10*1024*0.09 + 10*1024*0.085},
		{EgressInternet, 200 * 1024, 10*1024*0.09 + 40*1024*0.085 + 100*1024*0.07 + 50*1024*0.05},
		{EgressDirectConnect, 100, 2},
		{EgressSameRegion, 100, 0},
	} {
		got, err := p.egressCost(test.path, int64(test.gb*bytesPerGB))
		require.NoError(t, err)
		assert.InDelta(t, test.want, got, 1e-6, "%s %v GB", test.path, test.gb)
	}
	_, err := p.egressCost("carrier-pigeon", 1)
	assert.Error(t, err)
}

func TestMoneyJSON(t *testing.T) {
	data, err := json.Marshal(struct{ A, B, C Money }{1.006, 116.494, 0})
	require.NoError(t, err)
	assert.JSONEq(t, `{"A":1.01,"B":116.49,"C":0.00}`, string(data))
}

// TestEstimateDesignExample checks the example in the design document:
// 1.20 TiB in 6 Deep Archive objects, downloaded whole over the internet.
func TestEstimateDesignExample(t *testing.T) {
	bytes := int64(1319413953331) // 1.20 TiB
	est := &Estimate{Selection: Selection{
		Files:              2340,
		Bytes:              bytes,
		ObjectsToRestore:   6,
		BytesToRestore:     bytes,
		BytesToDownload:    bytes,
		DownloadRequests:   6,
		TemporaryCopyDays:  3,
		storageClassBytes:  map[string]int64{"DEEP_ARCHIVE": bytes},
		storageClassCounts: map[string]int{"DEEP_ARCHIVE": 6},
	}}
	require.NoError(t, est.options(&EstimateOptions{Prices: DefaultPrices(), EgressPath: EgressInternet}))
	require.Len(t, est.Options, 2)
	bulk, standard := est.Options[0], est.Options[1]
	assert.Equal(t, "Bulk", bulk.Tier)
	assert.Equal(t, 48, bulk.ReadyWithinHours)
	assert.InDelta(t, 3.07, float64(bulk.Costs.Retrieval), 0.005)
	assert.InDelta(t, 2.83, float64(bulk.Costs.TemporaryCopy), 0.005)
	assert.InDelta(t, 110.59, float64(bulk.Costs.Egress), 0.005)
	assert.InDelta(t, 116.49, float64(bulk.Total), 0.01)
	assert.InDelta(t, 5.90, float64(bulk.TotalWaived), 0.01)
	assert.Equal(t, "Standard", standard.Tier)
	assert.InDelta(t, 24.58, float64(standard.Costs.Retrieval), 0.005)
	assert.InDelta(t, 137.99, float64(standard.Total), 0.01)
	assert.InDelta(t, 27.40, float64(standard.TotalWaived), 0.01)

	// The JSON has the fields front ends rely on.
	data, err := json.Marshal(est)
	require.NoError(t, err)
	var decoded map[string]any
	require.NoError(t, json.Unmarshal(data, &decoded))
	for _, key := range []string{"prices", "egress", "selection", "options", "warnings"} {
		assert.Contains(t, decoded, key)
	}
	option := decoded["options"].([]any)[0].(map[string]any)
	for _, key := range []string{"tier", "label", "ready_within_hours", "costs", "total", "total_egress_waived"} {
		assert.Contains(t, option, key)
	}
}

func TestEstimateNothingToRetrieve(t *testing.T) {
	est := &Estimate{Selection: Selection{BytesToDownload: bytesPerGB, DownloadRequests: 1}}
	require.NoError(t, est.options(&EstimateOptions{Prices: DefaultPrices(), EgressPath: EgressInternet}))
	require.Len(t, est.Options, 1)
	assert.Equal(t, NoTier, est.Options[0].Tier)
	assert.InDelta(t, 0.09, float64(est.Options[0].Total), 1e-6)
	o, ok := est.Option("Bulk")
	assert.True(t, ok)
	assert.Equal(t, NoTier, o.Tier)
}

func TestEstimateMixedClasses(t *testing.T) {
	// Expedited isn't offered when some data is in Deep Archive.
	est := &Estimate{Selection: Selection{
		ObjectsToRestore:   2,
		storageClassBytes:  map[string]int64{"DEEP_ARCHIVE": 1, "GLACIER": 1},
		storageClassCounts: map[string]int{"DEEP_ARCHIVE": 1, "GLACIER": 1},
	}}
	require.NoError(t, est.options(&EstimateOptions{Prices: DefaultPrices(), EgressPath: EgressInternet}))
	var tiers []string
	for _, o := range est.Options {
		tiers = append(tiers, o.Tier)
	}
	assert.Equal(t, []string{"Bulk", "Standard"}, tiers)
	assert.Equal(t, 48, est.Options[0].ReadyWithinHours)
}

func TestLoadPrices(t *testing.T) {
	p := filepath.Join(t.TempDir(), "prices.json")
	require.NoError(t, os.WriteFile(p, []byte(`{"region":"eu-west-1","egress":{"internet":[{"up_to_gb":0,"per_gb":0.01}]}}`), 0o600))
	prices, err := LoadPrices(p)
	require.NoError(t, err)
	assert.Equal(t, "eu-west-1", prices.Region)
	assert.Equal(t, "USD", prices.Currency)
	assert.Contains(t, prices.Retrieval, "DEEP_ARCHIVE")
	cost, err := prices.egressCost(EgressInternet, 100*bytesPerGB)
	require.NoError(t, err)
	assert.InDelta(t, 1.0, cost, 1e-9)
}
