package gda

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
)

// Prices are the rates used to estimate the cost of a restore.
//
// The defaults are AWS list prices for us-east-1 and us-west-2 on Date.
// They change, so they can be replaced with a JSON file of the same shape.
type Prices struct {
	Region   string `json:"region"`
	Date     string `json:"date"`
	Currency string `json:"currency"`
	// Retrieval rates by storage class and then by retrieval tier.
	Retrieval map[string]map[string]RetrievalPrice `json:"retrieval"`
	// Storage of the temporary restored copy, per GB-month.
	TemporaryCopyPerGBMonth float64 `json:"temporary_copy_per_gb_month"`
	// GET requests, per 1,000.
	GetPer1000 float64 `json:"get_per_1000"`
	// Data transfer out by network path.
	Egress map[string][]EgressTier `json:"egress"`
	// Storage and upload rates by storage class, for backup estimates.
	Storage map[string]StoragePrice `json:"storage"`
}

// StoragePrice is the cost of storing objects in one storage class.
type StoragePrice struct {
	PerGBMonth float64 `json:"per_gb_month"`
	PutPer1000 float64 `json:"put_per_1000"`
	MinDays    int     `json:"min_days"` // minimum storage duration billed
	// S3 bills metadata for each archived object: OverheadKB at this
	// class's rate and OverheadStandardKB at the STANDARD rate.
	OverheadKB         float64 `json:"overhead_kb"`
	OverheadStandardKB float64 `json:"overhead_standard_kb"`
}

// RetrievalPrice is the price and speed of one retrieval tier.
type RetrievalPrice struct {
	PerGB      float64 `json:"per_gb"`
	Per1000    float64 `json:"per_1000_requests"`
	ReadyHours int     `json:"ready_within_hours"`
}

// EgressTier is a band of monthly data transfer out. UpToGB of 0 means
// no upper limit.
type EgressTier struct {
	UpToGB float64 `json:"up_to_gb"`
	PerGB  float64 `json:"per_gb"`
}

// Egress paths.
const (
	EgressInternet      = "internet"
	EgressDirectConnect = "direct-connect"
	EgressSameRegion    = "same-region"
)

// DefaultPrices returns the bundled price table.
func DefaultPrices() Prices {
	return Prices{
		Region:   "us-west-2",
		Date:     "2026-09-26",
		Currency: "USD",
		Retrieval: map[string]map[string]RetrievalPrice{
			"DEEP_ARCHIVE": {
				"Standard": {PerGB: 0.02, Per1000: 0.10, ReadyHours: 12},
				"Bulk":     {PerGB: 0.0025, Per1000: 0.025, ReadyHours: 48},
			},
			"GLACIER": {
				"Expedited": {PerGB: 0.03, Per1000: 10, ReadyHours: 1},
				"Standard":  {PerGB: 0.01, Per1000: 0.05, ReadyHours: 5},
				"Bulk":      {PerGB: 0, Per1000: 0, ReadyHours: 12},
			},
		},
		TemporaryCopyPerGBMonth: 0.023,
		GetPer1000:              0.0004,
		Egress: map[string][]EgressTier{
			EgressInternet: {
				{UpToGB: 10 * 1024, PerGB: 0.09},
				{UpToGB: 50 * 1024, PerGB: 0.085},
				{UpToGB: 150 * 1024, PerGB: 0.07},
				{UpToGB: 0, PerGB: 0.05},
			},
			EgressDirectConnect: {{UpToGB: 0, PerGB: 0.02}},
			EgressSameRegion:    {{UpToGB: 0, PerGB: 0}},
		},
		Storage: map[string]StoragePrice{
			"DEEP_ARCHIVE": {PerGBMonth: 0.00099, PutPer1000: 0.05, MinDays: 180, OverheadKB: 32, OverheadStandardKB: 8},
			"GLACIER":      {PerGBMonth: 0.0036, PutPer1000: 0.03, MinDays: 90, OverheadKB: 32, OverheadStandardKB: 8},
			"STANDARD":     {PerGBMonth: 0.023, PutPer1000: 0.005},
		},
	}
}

// LoadPrices reads a price table from a JSON file. Anything missing from
// the file keeps its default value: a retrieval tier replaces only that
// tier of that storage class, and an egress path only that path.
func LoadPrices(path string) (Prices, error) {
	prices := DefaultPrices()
	data, err := os.ReadFile(path)
	if err != nil {
		return prices, err
	}
	var file Prices
	if err := json.Unmarshal(data, &file); err != nil {
		return prices, fmt.Errorf("parse prices %q: %w", path, err)
	}
	if file.Region != "" {
		prices.Region = file.Region
	}
	if file.Date != "" {
		prices.Date = file.Date
	}
	if file.Currency != "" {
		prices.Currency = file.Currency
	}
	for class, tiers := range file.Retrieval {
		if prices.Retrieval[class] == nil {
			prices.Retrieval[class] = map[string]RetrievalPrice{}
		}
		for tier, price := range tiers {
			prices.Retrieval[class][tier] = price
		}
	}
	// Zero rates are valid, so these are only replaced when present.
	var present map[string]json.RawMessage
	_ = json.Unmarshal(data, &present)
	if _, ok := present["temporary_copy_per_gb_month"]; ok {
		prices.TemporaryCopyPerGBMonth = file.TemporaryCopyPerGBMonth
	}
	if _, ok := present["get_per_1000"]; ok {
		prices.GetPer1000 = file.GetPer1000
	}
	for path, tiers := range file.Egress {
		prices.Egress[path] = tiers
	}
	// A class keeps the defaults of the fields the file doesn't give.
	var storage struct {
		Storage map[string]json.RawMessage `json:"storage"`
	}
	_ = json.Unmarshal(data, &storage)
	for class, raw := range storage.Storage {
		price := prices.Storage[class]
		if err := json.Unmarshal(raw, &price); err != nil {
			return prices, fmt.Errorf("parse prices %q: %w", path, err)
		}
		prices.Storage[class] = price
	}
	return prices, nil
}

// BackupCost is the estimated cost of the data a backup run stored.
type BackupCost struct {
	Requests Money  `json:"requests"` // one off: uploads of data and metadata objects
	Monthly  Money  `json:"monthly"`  // storage per month of the data objects, with S3's per object overhead
	MinDays  int    `json:"min_days"` // storage of the data is billed for at least this many days
	Currency string `json:"currency"` // of the amounts
}

// EstimateBackup returns the cost of storing dataObjects objects of
// dataBytes in all in dataTier and writing metaObjects objects in
// metaTier. It returns false if either tier has no prices.
func (p *Prices) EstimateBackup(dataObjects, dataBytes, metaObjects int64, dataTier, metaTier string) (BackupCost, bool) {
	data, ok1 := p.Storage[canonicalStorageClass(dataTier)]
	meta, ok2 := p.Storage[canonicalStorageClass(metaTier)]
	standard := p.Storage["STANDARD"]
	if !ok1 || !ok2 {
		return BackupCost{}, false
	}
	objects := float64(dataObjects)
	gb := float64(dataBytes) / bytesPerGB
	gb += objects * data.OverheadKB * 1024 / bytesPerGB
	monthly := gb*data.PerGBMonth + objects*data.OverheadStandardKB*1024/bytesPerGB*standard.PerGBMonth
	requests := objects/1000*data.PutPer1000 + float64(metaObjects)/1000*meta.PutPer1000
	return BackupCost{Requests: Money(requests), Monthly: Money(monthly), MinDays: data.MinDays, Currency: p.Currency}, true
}

// canonicalStorageClass returns the S3 name of a storage class given in
// any case.
func canonicalStorageClass(class string) string {
	return strings.ToUpper(class)
}

// bytesPerGB is the size of a GB as AWS bills it.
const bytesPerGB = 1 << 30

// egressCost returns the cost of transferring bytes out over path.
func (p *Prices) egressCost(path string, bytes int64) (float64, error) {
	tiers, ok := p.Egress[path]
	if !ok {
		return 0, fmt.Errorf("no egress prices for network path %q", path)
	}
	gb := float64(bytes) / bytesPerGB
	cost, from := 0.0, 0.0
	for _, t := range tiers {
		upTo := t.UpToGB
		if upTo == 0 {
			upTo = math.Inf(1)
		}
		if gb > from {
			cost += (math.Min(gb, upTo) - from) * t.PerGB
		}
		if gb <= upTo {
			break
		}
		from = upTo
	}
	return cost, nil
}

// EstimateOptions configure a restore estimate.
type EstimateOptions struct {
	Prices     Prices
	EgressPath string // one of the Egress constants
	Waiver     bool   // whether egress is waived under a data egress waiver
}

// Estimate is the cost estimate of a restore, one option per retrieval
// tier. Its JSON form is what front ends such as Motuz use to build the
// restore dialog.
type Estimate struct {
	Prices    EstimatePrices `json:"prices"`
	Egress    EstimateEgress `json:"egress"`
	Selection Selection      `json:"selection"`
	Options   []Option       `json:"options"`
	Warnings  []string       `json:"warnings"`
}

// EstimatePrices says which prices an estimate used.
type EstimatePrices struct {
	Region   string `json:"region"`
	Date     string `json:"date"`
	Currency string `json:"currency"`
}

// EstimateEgress says how egress was costed.
type EstimateEgress struct {
	Path   string `json:"path"`
	Waiver bool   `json:"waiver"`
}

// Selection describes what a restore covers.
type Selection struct {
	Files              int   `json:"files"`
	Bytes              int64 `json:"bytes"`
	NoRetrievalFiles   int   `json:"already_restored_files"`
	ObjectsToRestore   int   `json:"objects_to_restore"`
	BytesToRestore     int64 `json:"bytes_to_restore"`
	BytesToDownload    int64 `json:"bytes_to_download"`
	DownloadRequests   int   `json:"download_requests"`
	TemporaryCopyDays  int   `json:"temporary_copy_days"`
	storageClassBytes  map[string]int64
	storageClassCounts map[string]int
}

// Option is the cost of restoring with one retrieval tier.
type Option struct {
	Tier             string `json:"tier"`
	Label            string `json:"label"`
	ReadyWithinHours int    `json:"ready_within_hours"`
	Costs            Costs  `json:"costs"`
	Total            Money  `json:"total"`
	TotalWaived      Money  `json:"total_egress_waived"`
}

// Costs are the components of an option's cost.
type Costs struct {
	Retrieval        Money `json:"retrieval"`
	RestoreRequests  Money `json:"restore_requests"`
	TemporaryCopy    Money `json:"temporary_copy"`
	DownloadRequests Money `json:"download_requests"`
	Egress           Money `json:"egress"`
}

// Money is an amount which is rounded to cents in JSON.
type Money float64

// MarshalJSON writes m rounded to cents.
func (m Money) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("%.2f", math.Round(float64(m)*100)/100)), nil
}

// NoTier is the option when nothing needs retrieving.
const NoTier = "None"

// tierOrder is the order options are listed in, cheapest first.
var tierOrder = []string{"Bulk", "Standard", "Expedited"}

// options works out the cost of each retrieval tier that every storage
// class in the selection supports.
func (est *Estimate) options(opt *EstimateOptions) error {
	sel := &est.Selection
	egress, err := opt.Prices.egressCost(opt.EgressPath, sel.BytesToDownload)
	if err != nil {
		return err
	}
	download := float64(sel.DownloadRequests) / 1000 * opt.Prices.GetPer1000
	if sel.ObjectsToRestore == 0 {
		o := Option{Tier: NoTier, Label: "Available now", Costs: Costs{DownloadRequests: Money(download), Egress: Money(egress)}}
		o.total()
		est.Options = []Option{o}
		return nil
	}
	temp := float64(sel.BytesToRestore) / bytesPerGB * opt.Prices.TemporaryCopyPerGBMonth * float64(sel.TemporaryCopyDays) / 30
	classes := make([]string, 0, len(sel.storageClassBytes))
	for class := range sel.storageClassBytes {
		classes = append(classes, class)
	}
	sort.Strings(classes)
	for _, tier := range tierOrder {
		o := Option{Tier: tier, Costs: Costs{TemporaryCopy: Money(temp), DownloadRequests: Money(download), Egress: Money(egress)}}
		supported := true
		for _, class := range classes {
			price, ok := opt.Prices.Retrieval[class][tier]
			if !ok {
				supported = false
				break
			}
			o.Costs.Retrieval += Money(float64(sel.storageClassBytes[class]) / bytesPerGB * price.PerGB)
			o.Costs.RestoreRequests += Money(float64(sel.storageClassCounts[class]) / 1000 * price.Per1000)
			o.ReadyWithinHours = max(o.ReadyWithinHours, price.ReadyHours)
		}
		if !supported {
			continue
		}
		o.Label = fmt.Sprintf("%s: ready within %d hours", tier, o.ReadyWithinHours)
		if o.ReadyWithinHours == 1 {
			o.Label = tier + ": ready within an hour"
		}
		o.total()
		est.Options = append(est.Options, o)
	}
	if len(est.Options) == 0 {
		return fmt.Errorf("no retrieval tier is priced for storage classes %s", strings.Join(classes, ", "))
	}
	return nil
}

func (o *Option) total() {
	c := o.Costs
	o.TotalWaived = c.Retrieval + c.RestoreRequests + c.TemporaryCopy + c.DownloadRequests
	o.Total = o.TotalWaived + c.Egress
}

// Option returns the option for tier, or false if there is none.
func (est *Estimate) Option(tier string) (Option, bool) {
	for _, o := range est.Options {
		if strings.EqualFold(o.Tier, tier) || o.Tier == NoTier {
			return o, true
		}
	}
	return Option{}, false
}
