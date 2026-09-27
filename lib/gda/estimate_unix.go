//go:build unix

package gda

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"

	"github.com/rclone/rclone/fs"
)

// EstimateRestore works out what restoring into target with ropt would
// cost with each retrieval tier. It reads only indexes and directory
// listings, so it costs nothing to run and requests no restores.
func EstimateRestore(ctx context.Context, dst fs.Fs, target string, ropt RestoreOptions, eopt EstimateOptions) (*Estimate, error) {
	at, err := ParseAt(ropt.At)
	if err != nil {
		return nil, err
	}
	d := &dest{f: dst, retries: 1}
	plan, err := buildPlan(ctx, newTree(d, at), ropt.Paths)
	if err != nil {
		return nil, err
	}
	conflicts, err := markIdentical(plan, target)
	if err != nil {
		return nil, err
	}
	est := &Estimate{
		Prices:   EstimatePrices{Region: eopt.Prices.Region, Date: eopt.Prices.Date, Currency: eopt.Prices.Currency},
		Egress:   EstimateEgress{Path: eopt.EgressPath, Waiver: eopt.Waiver},
		Warnings: []string{},
	}
	if len(conflicts) > 0 && !ropt.Overwrite {
		est.Warnings = append(est.Warnings, fmt.Sprintf("%d local files differ from the backup; restoring them needs --overwrite", len(conflicts)))
	}
	sel := &est.Selection
	sel.TemporaryCopyDays = ropt.Lifetime
	sel.storageClassBytes = map[string]int64{}
	sel.storageClassCounts = map[string]int{}

	byObject := map[string][]int{}
	for i := range plan {
		e := &plan[i]
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
	objects, err := listObjects(ctx, dst, byObject)
	if err != nil {
		return nil, err
	}
	for key, todo := range byObject {
		o, ok := objects[key]
		if !ok {
			est.Warnings = append(est.Warnings, fmt.Sprintf("%q is missing from the destination", key))
			continue
		}
		class := ""
		if t, ok := o.(fs.GetTierer); ok && dst.Features().GetTier {
			class = strings.ToUpper(t.GetTier())
		}
		if _, archival := eopt.Prices.Retrieval[class]; archival {
			sel.ObjectsToRestore++
			sel.BytesToRestore += o.Size()
			sel.storageClassBytes[class] += o.Size()
			sel.storageClassCounts[class]++
		} else {
			sel.NoRetrievalFiles += len(todo)
		}
		bytes, requests := downloadPlan(plan, todo, o.Size())
		sel.BytesToDownload += bytes
		sel.DownloadRequests += requests
	}
	if err := est.options(&eopt); err != nil {
		return nil, err
	}
	return est, nil
}

// downloadPlan returns the bytes and requests needed to fetch the plan
// entries at todo from an object of size bytes, choosing between ranged
// reads and a whole download as fetchObject does.
func downloadPlan(plan []Entry, todo []int, size int64) (bytes int64, requests int) {
	if plan[todo[0]].Offset < 0 {
		return size, 1
	}
	var want int64
	for _, i := range todo {
		want += plan[i].StoredLength
	}
	if len(todo) <= wholeObjectMembers && want*2 < size {
		return want, len(todo)
	}
	return size, 1
}

// listObjects returns the objects at the keys of byObject, listing each
// directory once rather than reading each object.
func listObjects(ctx context.Context, f fs.Fs, byObject map[string][]int) (map[string]fs.Object, error) {
	dirs := map[string]bool{}
	for key := range byObject {
		dir := path.Dir(key)
		if dir == "." {
			dir = ""
		}
		dirs[dir] = true
	}
	objects := map[string]fs.Object{}
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
