package promkv

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/api"
	promapi "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/prompb"
)

// KV is a key-value store backed by Prometheus.
type KV struct {
	opts Options
}

// Options configures the KV client.
type Options struct {
	APIURL   string
	WriteURL string

	Client *http.Client
}

type BasicAuth struct {
	Username, Password string
}

func New(opts Options) *KV {
	return &KV{opts: opts}
}

func (db *KV) Get(ctx context.Context, name string) ([]byte, error) {
	var _ promapi.API

	cli, err := api.NewClient(api.Config{
		Address: db.opts.APIURL,
		Client:  db.opts.Client,
	})
	if err != nil {
		return nil, err
	}

	api := promapi.NewAPI(cli)

	val, _, err := api.QueryRange(
		ctx,
		fmt.Sprintf("promkv_file_size_bytes{key=%q}", name),
		promapi.Range{
			Start: time.Now().UTC().Add(-time.Hour),
			End:   time.Now().UTC(),
			Step:  time.Minute,
		},
	)
	if err != nil {
		return nil, err
	}
	fileSizeBytes := getLastValue(val)

	val, _, err = api.QueryRange(
		ctx,
		fmt.Sprintf("promkv_file_timestamp_seconds{key=%q}", name),
		promapi.Range{
			Start: time.Now().UTC().Add(-time.Hour),
			End:   time.Now().UTC(),
			Step:  time.Minute,
		},
	)
	if err != nil {
		return nil, err
	}
	timestampSeconds := getLastValue(val)

	dataRange := promapi.Range{
		Start: timestamp.Time(int64(timestampSeconds)),
		End:   timestamp.Time(int64(timestampSeconds)).Add(time.Second * time.Duration(fileSizeBytes)),
		Step:  time.Second,
	}

	val, _, err = api.QueryRange(
		ctx,
		fmt.Sprintf("promkv_file_content{key=%q}", name),
		dataRange,
	)
	if err != nil {
		return nil, err
	}

	floatBytes := getValues(val)[:int(fileSizeBytes)]

	var bb []byte
	for _, fb := range floatBytes {
		bb = append(bb, byte(fb))
	}
	return bb, nil
}

func getValues(val model.Value) []float64 {
	switch val := val.(type) {
	case model.Matrix:
		samples := val[len(val)-1].Values

		var points []float64
		for _, sample := range samples {
			points = append(points, float64(sample.Value))
		}
		return points

	default:
		panic(fmt.Sprintf("Unrecognized type %T", val))
	}

}

func getLastValue(val model.Value) float64 {
	switch val := val.(type) {
	case model.Matrix:
		samples := val[len(val)-1].Values
		return float64(samples[len(samples)-1].Value)
	default:
		panic(fmt.Sprintf("Unrecognized type %T", val))
	}
}

func (db *KV) Set(ctx context.Context, name string, r io.Reader) error {
	req, err := buildWriteRequest(name, r)
	if err != nil {
		return err
	}

	pBuf := proto.NewBuffer(nil)
	if err := pBuf.Marshal(req); err != nil {
		return err
	}

	compressed := snappy.Encode(nil, pBuf.Bytes())

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, db.opts.WriteURL, bytes.NewReader(compressed))
	if err != nil {
		return err
	}

	httpReq.Header.Add("Content-Encoding", "snappy")
	httpReq.Header.Add("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("User-Agent", "promkv/v0.0.0")
	httpReq.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")

	cli := db.opts.Client
	if cli == nil {
		cli = http.DefaultClient
	}

	httpResp, err := cli.Do(httpReq)
	if err != nil {
		return err
	}
	defer func() {
		io.Copy(io.Discard, httpResp.Body)
		httpResp.Body.Close()
	}()

	if httpResp.StatusCode/100 != 2 {
		scanner := bufio.NewScanner(httpResp.Body)
		line := ""
		if scanner.Scan() {
			line = scanner.Text()
		}
		return fmt.Errorf("server returned HTTP status %s: %s", httpResp.Status, line)
	}

	return nil
}

func buildWriteRequest(name string, r io.Reader) (*prompb.WriteRequest, error) {
	req := &prompb.WriteRequest{
		Metadata: []prompb.MetricMetadata{
			{
				Type:             prompb.MetricMetadata_GAUGE,
				MetricFamilyName: "promkv_file_timestamp_seconds",
				Help:             "Last timestamp when file was written.",
				Unit:             "seconds",
			},
			{
				Type:             prompb.MetricMetadata_GAUGE,
				MetricFamilyName: "promkv_file_size_bytes",
				Help:             "Size of file.",
				Unit:             "bytes",
			},
			{
				Type:             prompb.MetricMetadata_GAUGE,
				MetricFamilyName: "promkv_file_content",
				Help:             "Content of file.",
			},
		},
	}

	bb, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	var (
		dataOffset = time.Second * time.Duration(len(bb))

		// The startTimestamp is the timestamp of the very first byte written.
		startTimestamp = time.Now().UTC().Add(-dataOffset)
	)

	req.Timeseries = append(req.Timeseries, prompb.TimeSeries{
		Labels: []prompb.Label{
			{Name: "__name__", Value: "promkv_file_timestamp_seconds"},
			{Name: "key", Value: name},
		},
		Samples: []prompb.Sample{{
			Timestamp: timestamp.FromTime(startTimestamp),
			Value:     float64(timestamp.FromTime(startTimestamp)),
		}},
	}, prompb.TimeSeries{
		Labels: []prompb.Label{
			{Name: "__name__", Value: "promkv_file_size_bytes"},
			{Name: "key", Value: name},
		},
		Samples: []prompb.Sample{{
			Timestamp: timestamp.FromTime(startTimestamp),
			Value:     float64(len(bb)),
		}},
	})

	contentSeries := prompb.TimeSeries{
		Labels: []prompb.Label{
			{Name: "__name__", Value: "promkv_file_content"},
			{Name: "key", Value: name},
		},
		Samples: make([]prompb.Sample, 0, len(bb)),
	}
	for i, b := range bb {
		offset := time.Second * time.Duration(i)

		contentSeries.Samples = append(contentSeries.Samples, prompb.Sample{
			Value:     float64(b),
			Timestamp: timestamp.FromTime(startTimestamp.Add(offset)),
		})
	}

	req.Timeseries = append(req.Timeseries, contentSeries)

	return req, nil
}
