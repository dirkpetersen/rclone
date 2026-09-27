package gda

import (
	"fmt"
	"strings"
	"time"
)

// tierHours is how long each retrieval tier takes at most for Deep Archive.
var tierHours = map[string]int{"Expedited": 5, "Standard": 12, "Bulk": 48}

// canonicalTier returns tier with the capitalisation S3 expects.
func canonicalTier(tier string) (string, error) {
	for t := range tierHours {
		if strings.EqualFold(t, tier) {
			return t, nil
		}
	}
	return "", fmt.Errorf("unknown restore tier %q: use Bulk, Standard or Expedited", tier)
}

// readyBy returns when a restore requested at now with tier should be
// readable, in whole seconds.
func readyBy(now time.Time, tier string) time.Time {
	return now.Add(time.Duration(tierHours[tier]) * time.Hour).UTC().Truncate(time.Second)
}
