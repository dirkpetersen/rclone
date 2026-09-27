//go:build unix

// Package restore implements 'rclone gda restore'.
package restore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/rclone/rclone/cmd"
	"github.com/rclone/rclone/cmd/gda"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/flags"
	libgda "github.com/rclone/rclone/lib/gda"
	"github.com/spf13/cobra"
)

var (
	opt        = libgda.DefaultRestoreOptions()
	resumeID   = ""
	jsonOut    = false
	yes        = false
	estimate   = false
	pricesFile = ""
	egressPath = libgda.EgressInternet
	waiver     = false
	maxCost    = 0.0
)

func init() {
	flagSet := Command.Flags()
	flags.StringVarP(flagSet, &opt.Tier, "tier", "", opt.Tier, "Retrieval tier: Bulk, Standard or Expedited", "")
	flags.IntVarP(flagSet, &opt.Lifetime, "lifetime", "", opt.Lifetime, "Days the restored copies stay available", "")
	flags.StringVarP(flagSet, &opt.At, "at", "", opt.At, "Restore as of this run ID or RFC 3339 time", "")
	flags.BoolVarP(flagSet, &opt.Overwrite, "overwrite", "", opt.Overwrite, "Replace local files which differ from the backup", "")
	flags.StringVarP(flagSet, &resumeID, "resume", "", resumeID, "Fetch what is ready of an earlier restore with this ID", "")
	flags.BoolVarP(flagSet, &jsonOut, "json", "", jsonOut, "Print the progress as JSON", "")
	flags.BoolVarP(flagSet, &yes, "yes", "", yes, "Don't ask for confirmation before requesting restores", "")
	flags.BoolVarP(flagSet, &estimate, "estimate", "", estimate, "Show the cost of each retrieval tier and exit", "")
	flags.StringVarP(flagSet, &pricesFile, "prices", "", pricesFile, "JSON file with the prices to use for estimates (default built in)", "")
	flags.StringVarP(flagSet, &egressPath, "egress-path", "", egressPath, "Network path data is downloaded over: internet, direct-connect or same-region", "")
	flags.BoolVarP(flagSet, &waiver, "egress-waiver", "", waiver, "Egress is waived under a data egress waiver", "")
	flags.Float64VarP(flagSet, &maxCost, "max-cost", "", maxCost, "Refuse to start if the estimated cost is higher (0 for no limit)", "")
	gda.Command.AddCommand(Command)
}

// Command is 'rclone gda restore'.
var Command = &cobra.Command{
	Use:   "restore <gda root> <target directory> [path ...]",
	Short: `Restore files from a GDA destination.`,
	Long: strings.ReplaceAll(`
Restores the given paths, or the whole tree if none are given, from the
GDA destination at gda root into the local target directory. Paths are
relative to the GDA root and keep that relative path below the target.

Data in an archive storage class must be restored before it can be
read, which takes up to 48 hours for Deep Archive with the default
!--tier Bulk!. So a restore runs in two steps:

    rclone gda restore s3:bucket/lab /restore results/plots
    rclone gda restore --resume <id> s3:bucket/lab

The first command requests the restores, saves the plan in the GDA root
under !_gda/restores/<id>! and fetches whatever is already readable.
Running it with !--resume <id>! fetches whatever has become readable
since and reports what is still being restored, so it is safe to run
from cron until it reports !done!.

Local files which are already identical to the backup are skipped. If
any local files differ, the restore stops before requesting anything
unless !--overwrite! is given.

Permissions and modification times are always restored, also on files
which were already identical. Owner, group, setuid, setgid and device
files are restored only when running as root, by name where the name
exists and by numeric ID otherwise. Nothing is written through symlinks
in the target directory: a symlink or file where the backup has a
directory is a conflict like any other.

If restored copies expire before they are fetched, !--resume! requests
them again.

With !--estimate! nothing is restored: it prints what each retrieval
tier would cost and how long it would take, broken down into retrieval,
restore requests, the temporary restored copy, download requests and
egress. !--egress-path! and !--egress-waiver! say how downloads are
charged, and !--prices! replaces the built in price table, which holds
AWS list prices as of the date it shows. !--max-cost! refuses to start a
restore whose estimate is higher, counting egress only without a waiver.

With !--json! the estimate and the progress are printed as JSON for
other programs, such as Motuz.
`, "!", "`"),
	Annotations: map[string]string{
		"versionIntroduced": "v1.76",
	},
	RunE: func(command *cobra.Command, args []string) error {
		var target string
		if resumeID != "" {
			cmd.CheckArgs(1, 2, command, args)
			if len(args) == 2 {
				target = args[1]
			}
		} else {
			cmd.CheckArgs(2, max(len(args), 2), command, args)
			target = args[1]
			opt.Paths = args[2:]
		}
		dst := cmd.NewFsDir(args[0:1])
		cmd.Run(false, false, command, func() error {
			ctx := context.Background()
			if resumeID != "" {
				st, err := libgda.ResumeRestore(ctx, dst, resumeID, target)
				if err != nil {
					return err
				}
				return report(st)
			}
			eopt := libgda.EstimateOptions{Prices: libgda.DefaultPrices(), EgressPath: egressPath, Waiver: waiver}
			if pricesFile != "" {
				prices, err := libgda.LoadPrices(pricesFile)
				if err != nil {
					return err
				}
				eopt.Prices = prices
			}
			est, err := libgda.EstimateRestore(ctx, dst, target, opt, eopt)
			if err != nil {
				return err
			}
			if estimate {
				return reportEstimate(est)
			}
			chosen, ok := est.Option(opt.Tier)
			if !ok {
				return fmt.Errorf("tier %s isn't available for this data", opt.Tier)
			}
			cost := chosen.Total
			if waiver {
				cost = chosen.TotalWaived
			}
			if maxCost > 0 && float64(cost) > maxCost {
				return fmt.Errorf("estimated cost %.2f %s is more than --max-cost %.2f", float64(cost), est.Prices.Currency, maxCost)
			}
			if !yes && !jsonOut && !fs.GetConfig(ctx).DryRun {
				if err := confirm(chosen, float64(cost), est.Prices.Currency); err != nil {
					return err
				}
			}
			st, err := libgda.StartRestore(ctx, dst, target, opt)
			if err != nil {
				return err
			}
			st.Estimate = &chosen
			return report(st)
		})
		return nil
	},
}

// confirm asks before requesting restores, which cost money.
func confirm(o libgda.Option, cost float64, currency string) error {
	fmt.Fprintf(os.Stderr, "%s, estimated cost %.2f %s. Start the restore? [y/N] ", o.Label, cost, currency)
	var answer string
	_, _ = fmt.Scanln(&answer)
	if !strings.EqualFold(answer, "y") && !strings.EqualFold(answer, "yes") {
		return errors.New("restore cancelled")
	}
	return nil
}

func report(st *libgda.RestoreStatus) error {
	if jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(st)
	}
	fmt.Printf("Restore %s (%s): %s\n", st.RestoreID, st.Tier, st.State)
	fmt.Printf("Objects: %d needed, %d fetched, %d still being restored\n", st.Objects.Requested, st.Objects.Fetched, st.Objects.Restoring)
	fmt.Printf("Files: %d total, %d in place (%d were already), %d failed\n", st.Files.Total, st.Files.Fetched, st.Files.SkippedIdentical, st.Files.Failed)
	if st.State == libgda.StateRestoring {
		fmt.Printf("Ready by %s. Fetch the rest with: rclone gda restore --resume %s <gda root>\n", st.ReadyBy.Local().Format("2006-01-02 15:04 MST"), st.RestoreID)
	}
	if st.State == libgda.StateFailed {
		return fmt.Errorf("%d files failed to restore", st.Files.Failed)
	}
	return nil
}

// reportEstimate prints the cost of each retrieval tier.
func reportEstimate(est *libgda.Estimate) error {
	if jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(est)
	}
	sel := est.Selection
	fmt.Printf("Restore %d files (%s), %d objects to restore (%s), %s to download\n",
		sel.Files, fs.SizeSuffix(sel.Bytes), sel.ObjectsToRestore, fs.SizeSuffix(sel.BytesToRestore), fs.SizeSuffix(sel.BytesToDownload))
	fmt.Printf("Prices: %s, %s, %s. Egress path: %s. Egress waiver: %v\n\n",
		est.Prices.Region, est.Prices.Date, est.Prices.Currency, est.Egress.Path, est.Egress.Waiver)
	fmt.Printf("%-10s %-10s %10s %10s %10s %10s %10s %10s %10s\n", "Option", "Ready in", "Retrieval", "Requests", "Temp copy", "Downloads", "Egress", "Total", "No egress")
	for _, o := range est.Options {
		ready := "now"
		if o.ReadyWithinHours > 0 {
			ready = fmt.Sprintf("%d hours", o.ReadyWithinHours)
		}
		c := o.Costs
		fmt.Printf("%-10s %-10s %10.2f %10.2f %10.2f %10.2f %10.2f %10.2f %10.2f\n", o.Tier, ready,
			float64(c.Retrieval), float64(c.RestoreRequests), float64(c.TemporaryCopy), float64(c.DownloadRequests), float64(c.Egress),
			float64(o.Total), float64(o.TotalWaived))
	}
	if sel.NoRetrievalFiles > 0 {
		fmt.Printf("\n%d files need no retrieval.\n", sel.NoRetrievalFiles)
	}
	for _, w := range est.Warnings {
		fmt.Printf("Warning: %s\n", w)
	}
	return nil
}
