// Copyright 2015 The Prometheus Authors
// Copyright 2017 Corentin Chary <corentin.chary@gmail.com>
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package graphite

import (
	"bytes"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/url"
	"io/ioutil"
	"time"
	"encoding/json"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/prometheus/common/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/remote"

	"golang.org/x/net/context"
	"golang.org/x/net/context/ctxhttp"

	"github.com/criteo/graphite-remote-adapter/graphite/config"
)

const (
	expandEndpoint = "/metrics/expand"
	renderEndpoint = "/render/"
)

// Client allows sending batches of Prometheus samples to Graphite.
type Client struct {
	carbon           string
	carbon_transport string
	write_timeout    time.Duration
	graphite_web     string
	read_timeout     time.Duration
	prefix           string
	rules            []*config.Rule
	template_data    map[string]interface{}
	ignoredSamples   prometheus.Counter
}

// NewClient creates a new Client.
func NewClient(carbon string, carbon_transport string, write_timeout time.Duration,
	graphite_web string, read_timeout time.Duration, prefix string, configFile string) *Client {
	fileConf, err := config.LoadFile(configFile)
	if err != nil {
		log.With("err", err).Warnln("Error loading config file")
	}
	return &Client{
		carbon:           carbon,
		carbon_transport: carbon_transport,
		write_timeout:    write_timeout,
		graphite_web:     graphite_web,
		read_timeout:     read_timeout,
		prefix:           prefix,
		rules:            fileConf.Rules,
		template_data:    fileConf.Template_data,
		ignoredSamples: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "prometheus_influxdb_ignored_samples_total",
				Help: "The total number of samples not sent to InfluxDB due to unsupported float values (Inf, -Inf, NaN).",
			},
		),
	}
}

func prepareDataPoint(path string, s *model.Sample, c *Client) string {
	t := float64(s.Timestamp.UnixNano()) / 1e9
	v := float64(s.Value)
	if math.IsNaN(v) || math.IsInf(v, 0) {
		log.Debugf("cannot send value %f to Graphite,"+
			"skipping sample %#v", v, s)
		c.ignoredSamples.Inc()
		return ""
	}
	return fmt.Sprintf("%s %f %f\n", path, v, t)
}

// Write sends a batch of samples to Graphite.
func (c *Client) Write(samples model.Samples) error {
	log.With("num_samples", len(samples)).With("storage", c.Name()).Debugf("Remote write")
	if c.carbon == "" {
		return nil
	}

	conn, err := net.DialTimeout(c.carbon_transport, c.carbon, c.write_timeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	var buf bytes.Buffer
	for _, s := range samples {
		paths := pathsFromMetric(s.Metric, c.prefix, c.rules, c.template_data)
		for _, k := range paths {
			if str := prepareDataPoint(k, s, c); str != "" {
				fmt.Fprintf(&buf, str)
			}
		}
	}
	fmt.Println(buf.String())

	_, err = conn.Write(buf.Bytes())
	if err != nil {
		return err
	}

	return nil
}

type RenderResponse struct {
	Target     string       `yaml:"target,omitempty" json:"target,omitempty"`
	Datapoints []*Datapoint `yaml:"datapoints,omitempty" json:"datapoints,omitempty"`
}

type Datapoint struct {
	Value       *float64
	Timestamp   int64
}

func (d *Datapoint) UnmarshalJSON(b []byte) error {
    var x []interface{}
    err := json.Unmarshal(b, &x)
    if err != nil {
			return err
    }
		if x[0] != nil {
			d.Value = x[0].(*float64)
		}
		d.Timestamp = int64(x[1].(float64))
		return nil
}

func prepareUrl(host string, path string, params map[string]string) *url.URL {
	values := url.Values{}
	for k, v := range params {
		values.Set(k, v)
	}
	return &url.URL{
		Scheme: "http",
		Host: host,
		Path: path,
		ForceQuery: true,
		RawQuery: values.Encode(),
	}
}

func fetchUrl(u *url.URL, ctx context.Context) ([]byte, error) {
	hresp, err := ctxhttp.Get(ctx, http.DefaultClient, u.String())
	if err != nil {
		return nil, err
	}
	defer hresp.Body.Close()

	body, err := ioutil.ReadAll(hresp.Body)
	if err != nil {
		return nil, err
	}
	return body, nil
}

func (c *Client) Read(req *remote.ReadRequest) (*remote.ReadResponse, error) {
	log.With("req", req).Debugf("Remote read")

	if c.graphite_web == "" {
		return nil, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.read_timeout)
	defer cancel()

	for _, query := range req.Queries {
		from := query.StartTimestampMs/1000
		until := query.EndTimestampMs/1000
		expandUrl := prepareUrl(c.graphite_web,
										expandEndpoint,
										map[string]string{"format": "json",
																			"leavesOnly": "1",
																			"query": "local.**",
																		},
									)
		// - query=<prefix>.<__name__>.**

		body, err := fetchUrl(expandUrl, ctx)
		expandResp := make(map[string]interface{})
		err = json.Unmarshal(body, &expandResp)
		if err != nil {
			log.With("url", expandUrl).With("err", err).Warnln("Error parsing exand endpoint response body")
			continue
		}
		if _, ok := expandResp["results"]; !ok {
			log.With("url", expandUrl).Warnln("No 'results' key in exand endpoint response")
			continue
		}

		for _, leaf := range expandResp["results"].([]interface{}) {
			renderUrl := prepareUrl(c.graphite_web,
											 renderEndpoint,
											 map[string]string{"format": "json",
																				"from": strconv.FormatInt(from, 10),
																				"until": strconv.FormatInt(until, 10),
																				"target": leaf.(string),
											  								},
										 )
			body, err := fetchUrl(renderUrl, ctx)
			renderResp := make([]RenderResponse, 0)
			err = json.Unmarshal(body, &renderResp)
			if err != nil {
				log.With("url", renderUrl).With("err", err).Warnln("Error parsing render endpoint response body")
				continue
			}
			if len(renderResp) == 0 {
				log.With("url", renderUrl).Warnln("Empty render endpoint response")
				continue
			}
			result := renderResp[0]
			fmt.Println(result.Target)
			for _, datapoint := range result.Datapoints {
				fmt.Println(datapoint.Value, " -> ", datapoint.Timestamp)
			}
		}
	}

	// TODO: Do post-filtering here and filter the right names, build TimeSeries.
	// TODO: For each metric, get data (http request to /render?format=json)
	// TODO: Parse data and build Samples.

	resp := remote.ReadResponse{
		Results: []*remote.QueryResult{
			{Timeseries: make([]*remote.TimeSeries, 0, 0)},
		},
	}

	return &resp, nil
}

// Name identifies the client as a Graphite client.
func (c Client) Name() string {
	return "graphite"
}
