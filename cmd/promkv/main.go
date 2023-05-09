package main

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"os"

	"github.com/prometheus/common/config"
	"github.com/rfratto/promkv"
	"github.com/spf13/cobra"
)

func main() {
	db := promkv.New(promkv.Options{
		APIURL:   os.Getenv("PROMETHEUS_URL"),
		WriteURL: os.Getenv("PROMETHEUS_REMOTE_WRITE_URL"),

		Client: &http.Client{
			Transport: config.NewBasicAuthRoundTripper(
				os.Getenv("PROMETHEUS_USERNAME"),
				config.Secret(os.Getenv("PROMETHEUS_PASSWORD")),
				"", // No password file
				http.DefaultTransport,
			),
		},
	})

	cmd := &cobra.Command{
		Use:   "promkv",
		Short: "Awful key-value storage backed by Prometheus.",

		Long: `promkv is a key-value storage backed by Prometheus.

It is meant to be used as a joke, and should not be used for real persistent storage. Expect data corruption and bugs to be rampant.

promkv looks at the following environment variables:

* PROMETHEUS_URL: The base URL of the Prometheus API. 
* PROMETHEUS_REMOTE_WRITE_URL: The URL to remote_write values to.
* PROMETHEUS_USERNAME: Optional remote_write username.
* PROMETHEUS_PASSWORD: Optional remote_write password.`,
	}

	var (
		getCmd = &cobra.Command{
			Use:   "get [name]",
			Short: "Get a value from promkv.",
			Args:  cobra.ExactArgs(1),

			RunE: func(_ *cobra.Command, args []string) error {
				bb, err := db.Get(context.Background(), args[0])
				if err != nil {
					return err
				}

				_, _ = io.Copy(os.Stdout, bytes.NewReader(bb))
				return nil
			},
		}

		setCmd = &cobra.Command{
			Use:   "set [name]",
			Short: "Store a value in promkv. Data is read from stdin.",
			Args:  cobra.ExactArgs(1),

			RunE: func(cmd *cobra.Command, args []string) error {
				return db.Set(context.Background(), args[0], os.Stdin)
			},
		}
	)

	cmd.AddCommand(getCmd, setCmd)

	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
