package gda

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
)

// priceListURL is the AWS Price List bulk API offer file of a service in
// a region. Tests replace it.
var priceListURL = "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/%s/current/%s/index.json"

// offerFile is the part of an AWS Price List offer file that is used.
type offerFile struct {
	PublicationDate string `json:"publicationDate"`
	Products        map[string]struct {
		Attributes map[string]string `json:"attributes"`
	} `json:"products"`
	Terms struct {
		OnDemand map[string]map[string]struct {
			PriceDimensions map[string]priceDimension `json:"priceDimensions"`
		} `json:"OnDemand"`
	} `json:"terms"`
}

// priceDimension is one band of a price.
type priceDimension struct {
	BeginRange   string            `json:"beginRange"`
	EndRange     string            `json:"endRange"`
	Unit         string            `json:"unit"`
	PricePerUnit map[string]string `json:"pricePerUnit"`
}

// band is a parsed price dimension. end is 0 for no upper limit.
type band struct {
	begin, end, price float64
	unit              string
}

// fetchOffer downloads the offer file of service in region.
func fetchOffer(ctx context.Context, client *http.Client, service, region string) (*offerFile, error) {
	url := fmt.Sprintf(priceListURL, service, region)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("get %s: %s", url, resp.Status)
	}
	var offer offerFile
	if err := json.NewDecoder(resp.Body).Decode(&offer); err != nil {
		return nil, fmt.Errorf("parse %s: %w", url, err)
	}
	return &offer, nil
}

// bands returns the price bands in USD of the product sku, lowest first.
func (o *offerFile) bands(sku string) ([]band, error) {
	var out []band
	for _, term := range o.Terms.OnDemand[sku] {
		for _, d := range term.PriceDimensions {
			var b band
			var err1, err2, err3 error
			b.begin, err1 = strconv.ParseFloat(d.BeginRange, 64)
			if d.EndRange != "Inf" {
				b.end, err2 = strconv.ParseFloat(d.EndRange, 64)
			}
			b.price, err3 = strconv.ParseFloat(d.PricePerUnit["USD"], 64)
			if err := errors.Join(err1, err2, err3); err != nil {
				return nil, fmt.Errorf("price of %s: %w", sku, err)
			}
			b.unit = d.Unit
			out = append(out, b)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].begin < out[j].begin })
	return out, nil
}

// find returns the price bands of the product whose attributes match
// all of want, with the usage type compared without its region prefix,
// such as "USW2-". It checks the bands are in unit.
func (o *offerFile) find(want map[string]string, unit string) ([]band, error) {
	for sku, p := range o.Products {
		match := true
		for k, v := range want {
			got := p.Attributes[k]
			if k == "usagetype" {
				got = trimRegionPrefix(got)
			}
			if got != v {
				match = false
				break
			}
		}
		if !match {
			continue
		}
		bands, err := o.bands(sku)
		if err != nil {
			return nil, err
		}
		if len(bands) == 0 {
			break
		}
		for _, b := range bands {
			if b.unit != unit {
				return nil, fmt.Errorf("price of %v is per %q, not %q", want, b.unit, unit)
			}
		}
		return bands, nil
	}
	return nil, fmt.Errorf("no price for %v", want)
}

// trimRegionPrefix removes the region prefix, such as "USW2-" or "EU-",
// from a usage type. us-east-1 usage types mostly have none.
func trimRegionPrefix(usage string) string {
	prefix, rest, ok := strings.Cut(usage, "-")
	if !ok || strings.Trim(prefix, "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789") != "" {
		return usage
	}
	return rest
}

// roundPrice removes floating point noise from a price.
func roundPrice(p float64) float64 {
	return math.Round(p*1e10) / 1e10
}

// FetchPrices returns the price table for an AWS region, starting from
// the defaults and updating the rates the AWS Price List publishes:
// retrieval per GB, Flexible Retrieval request fees, the temporary copy,
// GET requests, internet egress, and storage and uploads in STANDARD and
// GLACIER. Deep Archive storage, upload and restore request fees aren't
// in the price list, so they keep their default values.
func FetchPrices(ctx context.Context, client *http.Client, region string) (Prices, error) {
	prices := DefaultPrices()
	s3, err := fetchOffer(ctx, client, "AmazonS3", region)
	if err != nil {
		return prices, err
	}
	transfer, err := fetchOffer(ctx, client, "AWSDataTransfer", region)
	if err != nil {
		return prices, err
	}
	prices.Region = region
	prices.Date, _, _ = strings.Cut(s3.PublicationDate, "T")
	prices.Currency = "USD"
	first := func(usage, operation, unit string) (float64, error) {
		bands, err := s3.find(map[string]string{"regionCode": region, "usagetype": usage, "operation": operation}, unit)
		if err != nil {
			return 0, err
		}
		return roundPrice(bands[0].price), nil
	}
	var errs []error
	retrieval := func(class, tier, usage, operation string) {
		p := prices.Retrieval[class][tier]
		var err error
		if p.PerGB, err = first(usage, operation, "GB"); err != nil {
			errs = append(errs, err)
		}
		prices.Retrieval[class][tier] = p
	}
	retrieval("DEEP_ARCHIVE", "Standard", "Standard-Retrieval-Bytes", "DeepArchiveRestoreObject")
	retrieval("DEEP_ARCHIVE", "Bulk", "Bulk-Retrieval-Bytes", "DeepArchiveRestoreObject")
	retrieval("GLACIER", "Expedited", "Expedited-Retrieval-Bytes", "")
	retrieval("GLACIER", "Standard", "Standard-Retrieval-Bytes", "RestoreObject")
	retrieval("GLACIER", "Bulk", "Bulk-Retrieval-Bytes", "RestoreObject")
	requests := func(tier, usage, operation string) {
		p := prices.Retrieval["GLACIER"][tier]
		perRequest, err := first(usage, operation, "Requests")
		if err != nil {
			errs = append(errs, err)
		}
		p.Per1000 = roundPrice(perRequest * 1000)
		prices.Retrieval["GLACIER"][tier] = p
	}
	requests("Expedited", "Requests-Tier6", "")
	requests("Standard", "Requests-Tier3", "RestoreObject")
	requests("Bulk", "Requests-Tier5", "")
	if prices.TemporaryCopyPerGBMonth, err = first("TimedStorage-ByteHrs", "", "GB-Mo"); err != nil {
		errs = append(errs, err)
	}
	get, err := first("Requests-Tier2", "", "Requests")
	if err != nil {
		errs = append(errs, err)
	}
	prices.GetPer1000 = roundPrice(get * 1000)
	storage := func(class, usage, putUsage, putOperation string) {
		p := prices.Storage[class]
		var err error
		if p.PerGBMonth, err = first(usage, "", "GB-Mo"); err != nil {
			errs = append(errs, err)
		}
		put, err := first(putUsage, putOperation, "Requests")
		if err != nil {
			errs = append(errs, err)
		}
		p.PutPer1000 = roundPrice(put * 1000)
		prices.Storage[class] = p
	}
	storage("STANDARD", "TimedStorage-ByteHrs", "Requests-Tier1", "")
	storage("GLACIER", "TimedStorage-GlacierByteHrs", "Requests-GLACIER-Tier1", "PutObject")
	egress, err := transfer.find(map[string]string{
		"fromRegionCode": region,
		"transferType":   "AWS Outbound",
		"toLocation":     "External",
		"usagetype":      "DataTransfer-Out-Bytes",
	}, "GB")
	if err != nil {
		errs = append(errs, err)
	} else {
		var tiers []EgressTier
		for _, b := range egress {
			tiers = append(tiers, EgressTier{UpToGB: b.end, PerGB: roundPrice(b.price)})
		}
		prices.Egress[EgressInternet] = tiers
	}
	if err := errors.Join(errs...); err != nil {
		return prices, fmt.Errorf("prices for %s: %w", region, err)
	}
	return prices, nil
}
