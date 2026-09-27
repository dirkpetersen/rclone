package gda

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testOffer builds an offer file holding products, each given as its
// attributes and price bands of begin, end, price and unit.
func testOffer(products []map[string]string, bands [][][4]string) offerFile {
	var o offerFile
	o.PublicationDate = "2026-09-26T01:55:12Z"
	o.Products = map[string]struct {
		Attributes map[string]string `json:"attributes"`
	}{}
	o.Terms.OnDemand = map[string]map[string]struct {
		PriceDimensions map[string]priceDimension `json:"priceDimensions"`
	}{}
	for i, attrs := range products {
		sku := fmt.Sprintf("SKU%d", i)
		o.Products[sku] = struct {
			Attributes map[string]string `json:"attributes"`
		}{attrs}
		dims := map[string]priceDimension{}
		for j, b := range bands[i] {
			dims[fmt.Sprintf("%s.%d", sku, j)] = priceDimension{BeginRange: b[0], EndRange: b[1], PricePerUnit: map[string]string{"USD": b[2]}, Unit: b[3]}
		}
		o.Terms.OnDemand[sku] = map[string]struct {
			PriceDimensions map[string]priceDimension `json:"priceDimensions"`
		}{sku + ".T": {PriceDimensions: dims}}
	}
	return o
}

func TestFetchPrices(t *testing.T) {
	s3 := func(usage, op string) map[string]string {
		return map[string]string{"regionCode": "us-west-2", "usagetype": usage, "operation": op}
	}
	one := func(price, unit string) [][4]string { return [][4]string{{"0", "Inf", price, unit}} }
	s3Offer := testOffer([]map[string]string{
		s3("USW2-Standard-Retrieval-Bytes", "DeepArchiveRestoreObject"),
		s3("USW2-Bulk-Retrieval-Bytes", "DeepArchiveRestoreObject"),
		s3("USW2-Expedited-Retrieval-Bytes", ""),
		s3("USW2-Standard-Retrieval-Bytes", "RestoreObject"),
		s3("USW2-Bulk-Retrieval-Bytes", "RestoreObject"),
		s3("USW2-Requests-Tier6", ""),
		s3("USW2-Requests-Tier3", "RestoreObject"),
		s3("USW2-Requests-Tier5", ""),
		s3("USW2-TimedStorage-ByteHrs", ""),
		s3("USW2-Requests-Tier2", ""),
		// Look-alikes which mustn't be picked.
		s3("USW2-Standard-Retrieval-Bytes", "IntDAARestoreObject"),
		s3("USW2-Requests-INT-Tier2", ""),
	}, [][][4]string{
		one("0.0300000000", "GB"),
		one("0.0035000000", "GB"),
		one("0.0400000000", "GB"),
		one("0.0110000000", "GB"),
		one("0.0000000000", "GB"),
		one("0.0100000000", "Requests"),
		one("0.0000500000", "Requests"),
		one("0.0000000000", "Requests"),
		{{"51200", "512000", "0.022", "GB-Mo"}, {"0", "51200", "0.0230000000", "GB-Mo"}},
		one("0.0000004000", "Requests"),
		one("9", "GB"),
		one("9", "Requests"),
	})
	transferOffer := testOffer([]map[string]string{
		{"fromRegionCode": "us-west-2", "transferType": "AWS Outbound", "toLocation": "External", "usagetype": "USW2-DataTransfer-Out-Bytes"},
		{"fromRegionCode": "us-west-2", "transferType": "InterRegion Outbound", "toLocation": "US East (Ohio)", "usagetype": "USW2-USE2-AWS-Out-Bytes"},
	}, [][][4]string{
		{{"0", "10240", "0.09", "GB"}, {"10240", "51200", "0.085", "GB"}, {"51200", "Inf", "0.05", "GB"}},
		one("0.02", "GB"),
	})
	offers := map[string]offerFile{"AmazonS3": s3Offer, "AWSDataTransfer": transferOffer}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(r.URL.Path, "/")
		if len(parts) < 3 || parts[len(parts)-2] != "us-west-2" {
			http.NotFound(w, r)
			return
		}
		offer, ok := offers[parts[1]]
		if !ok {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(offer)
	}))
	defer srv.Close()
	old := priceListURL
	priceListURL = srv.URL + "/%s/%s/index.json"
	defer func() { priceListURL = old }()

	p, err := FetchPrices(context.Background(), srv.Client(), "us-west-2")
	require.NoError(t, err)
	assert.Equal(t, "us-west-2", p.Region)
	assert.Equal(t, "2026-09-26", p.Date)
	da := p.Retrieval["DEEP_ARCHIVE"]
	assert.Equal(t, 0.03, da["Standard"].PerGB)
	assert.Equal(t, 0.0035, da["Bulk"].PerGB)
	// Deep Archive request fees aren't published, so keep the defaults.
	assert.Equal(t, DefaultPrices().Retrieval["DEEP_ARCHIVE"]["Bulk"].Per1000, da["Bulk"].Per1000)
	assert.Equal(t, 48, da["Bulk"].ReadyHours)
	fl := p.Retrieval["GLACIER"]
	assert.Equal(t, 0.04, fl["Expedited"].PerGB)
	assert.Equal(t, 10.0, fl["Expedited"].Per1000)
	assert.Equal(t, 0.011, fl["Standard"].PerGB)
	assert.Equal(t, 0.05, fl["Standard"].Per1000)
	assert.Equal(t, 0.0, fl["Bulk"].Per1000)
	assert.Equal(t, 0.023, p.TemporaryCopyPerGBMonth)
	assert.Equal(t, 0.0004, p.GetPer1000)
	assert.Equal(t, []EgressTier{{UpToGB: 10240, PerGB: 0.09}, {UpToGB: 51200, PerGB: 0.085}, {UpToGB: 0, PerGB: 0.05}}, p.Egress[EgressInternet])
	assert.Equal(t, DefaultPrices().Egress[EgressDirectConnect], p.Egress[EgressDirectConnect])

	// A region without a price list fails.
	_, err = FetchPrices(context.Background(), srv.Client(), "xx-nowhere-1")
	assert.ErrorContains(t, err, "404")
}

func TestTrimRegionPrefix(t *testing.T) {
	assert.Equal(t, "Requests-Tier2", trimRegionPrefix("USW2-Requests-Tier2"))
	assert.Equal(t, "Requests-Tier2", trimRegionPrefix("EU-Requests-Tier2"))
	assert.Equal(t, "Requests-Tier2", trimRegionPrefix("Requests-Tier2"))
	assert.Equal(t, "TimedStorage-ByteHrs", trimRegionPrefix("TimedStorage-ByteHrs"))
}
