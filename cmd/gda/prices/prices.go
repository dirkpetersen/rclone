// Package prices implements 'rclone gda prices'.
package prices

import (
	"context"
	"encoding/json"
	"os"
	"strings"

	"github.com/rclone/rclone/cmd"
	"github.com/rclone/rclone/cmd/gda"
	"github.com/rclone/rclone/fs/config/flags"
	"github.com/rclone/rclone/fs/fshttp"
	libgda "github.com/rclone/rclone/lib/gda"
	"github.com/spf13/cobra"
)

var region = libgda.DefaultPrices().Region

func init() {
	flags.StringVarP(Command.Flags(), &region, "region", "", region, "AWS region to get prices for", "")
	gda.Command.AddCommand(Command)
}

// Command is 'rclone gda prices'.
var Command = &cobra.Command{
	Use:   "prices",
	Short: `Print current AWS prices for GDA restore estimates.`,
	Long: strings.ReplaceAll(`
Downloads the current list prices for !--region! from the AWS Price List
and prints them as a price table for !rclone gda restore --prices!:

    rclone gda prices --region us-east-1 > prices.json
    rclone gda restore s3:bucket/lab /restore --estimate --prices prices.json

It updates the retrieval rates per GB, the Flexible Retrieval request
fees, the rate of the temporary restored copy, GET requests and egress
to the internet. The Price List doesn't list Deep Archive restore
request fees, nor Direct Connect egress, which depends on the location,
so those keep their built in values; edit the file to change them.
`, "!", "`"),
	Annotations: map[string]string{
		"versionIntroduced": "v1.76",
	},
	RunE: func(command *cobra.Command, args []string) error {
		cmd.CheckArgs(0, 0, command, args)
		cmd.Run(false, false, command, func() error {
			ctx := context.Background()
			prices, err := libgda.FetchPrices(ctx, fshttp.NewClient(ctx), region)
			if err != nil {
				return err
			}
			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")
			return enc.Encode(prices)
		})
		return nil
	},
}
