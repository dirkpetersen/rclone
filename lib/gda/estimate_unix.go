//go:build unix

package gda

import (
	"context"
	"errors"
	"fmt"
	"path"

	"github.com/rclone/rclone/fs"
)

// Estimate works out what the planned restore would cost with each
// retrieval tier. It reads only directory listings and the first byte of
// each archived object, to tell which are restored already, so it costs
// next to nothing and requests no restores.
func (p *RestorePlan) Estimate(ctx context.Context, eopt EstimateOptions) (*Estimate, error) {
	est := &Estimate{
		Prices:   EstimatePrices{Region: eopt.Prices.Region, Date: eopt.Prices.Date, Currency: eopt.Prices.Currency},
		Egress:   EstimateEgress{Path: eopt.EgressPath, Waiver: eopt.Waiver},
		Warnings: []string{},
	}
	if len(p.conflicts) > 0 && !p.opt.Overwrite {
		est.Warnings = append(est.Warnings, fmt.Sprintf("%d local files differ from the backup; restoring them needs --overwrite", len(p.conflicts)))
	}
	objects, err := p.findObjects(ctx)
	if err != nil {
		return nil, err
	}
	sel := &est.Selection
	sel.TemporaryCopyDays = p.opt.Lifetime
	sel.storageClassBytes = map[string]int64{}
	sel.storageClassCounts = map[string]int{}
	byObject := map[string][]int{}
	for i := range p.entries {
		e := &p.entries[i]
		if e.Type != TypeFile {
			continue
		}
		sel.Files++
		sel.Bytes += e.Size
		if !needsData(e) {
			sel.NoRetrievalFiles++
			continue
		}
		byObject[e.Location] = append(byObject[e.Location], i)
	}
	for key, todo := range byObject {
		po, ok := objects[key]
		if !ok {
			est.Warnings = append(est.Warnings, fmt.Sprintf("%q is missing from the destination", key))
			continue
		}
		_, priced := eopt.Prices.Retrieval[po.class]
		if priced && !po.readable {
			sel.ObjectsToRestore++
			sel.BytesToRestore += po.o.Size()
			sel.storageClassBytes[po.class] += po.o.Size()
			sel.storageClassCounts[po.class]++
		} else {
			// Not archived, or restored already.
			sel.NoRetrievalFiles += len(todo)
		}
		bytes, requests := downloadPlan(p.entries, todo, po.o.Size())
		sel.BytesToDownload += bytes
		sel.DownloadRequests += requests
	}
	if err := est.options(&eopt); err != nil {
		return nil, err
	}
	return est, nil
}

// EstimateRestore plans a restore and estimates its cost; see
// PlanRestore and RestorePlan.Estimate.
func EstimateRestore(ctx context.Context, dst fs.Fs, target string, ropt RestoreOptions, eopt EstimateOptions) (*Estimate, error) {
	p, err := PlanRestore(ctx, dst, target, ropt)
	if err != nil {
		return nil, err
	}
	return p.Estimate(ctx, eopt)
}

// downloadPlan returns the bytes and requests needed to fetch the plan
// entries at todo from an object of size bytes, choosing between ranged
// reads and a whole download as fetchObject does.
func downloadPlan(plan []Entry, todo []int, size int64) (bytes int64, requests int) {
	if plan[todo[0]].Offset < 0 {
		return size, 1
	}
	spans := packSpans(plan, todo)
	if !useSpans(spans, size) {
		return size, 1
	}
	for _, s := range spans {
		bytes += s.end - s.start
	}
	return bytes, len(spans)
}

// listObjects returns the objects at the keys of byObject, listing each
// directory once rather than reading each object.
func listObjects(ctx context.Context, f fs.Fs, byObject map[string][]int) (map[string]fs.Object, error) {
	dirs := map[string]bool{}
	objects := map[string]fs.Object{}
	for key := range byObject {
		if _, version := splitVersionKey(key); version != "" {
			// A listing shows only the current version.
			o, err := newDataObject(ctx, f, key)
			if errors.Is(err, fs.ErrorObjectNotFound) {
				continue
			}
			if err != nil {
				return nil, err
			}
			objects[key] = o
			continue
		}
		dir := path.Dir(key)
		if dir == "." {
			dir = ""
		}
		dirs[dir] = true
	}
	for dir := range dirs {
		entries, err := f.List(ctx, dir)
		if errors.Is(err, fs.ErrorDirNotFound) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("list %q: %w", dir, err)
		}
		for _, entry := range entries {
			if o, ok := entry.(fs.Object); ok {
				if _, wanted := byObject[o.Remote()]; wanted {
					objects[o.Remote()] = o
				}
			}
		}
	}
	return objects, nil
}
