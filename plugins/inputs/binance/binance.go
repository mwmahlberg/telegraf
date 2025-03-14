//go:generate ../../../tools/readme_config_includer/generator
package binance

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/plugins/inputs"
)

const (
	baseApiUrlString      string = "https://api.binance.com/api/v3"
	priceUrlString        string = baseApiUrlString + "/ticker/price"
	exchangeInfoUrlString string = baseApiUrlString + "/exchangeInfo"
)

type payload struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
}

type tick struct {
	Symbol string `json:"symbol"`
	Price  string `json:"price"`
}

//go:embed sample.conf
var sampleConfig string

var (
	header map[string][]string = map[string][]string{
		"User-Agent":   {"Telegraf"},
		"Accept":       {"application/json"},
		"Content-Type": {"application/json"},
	}
)

type Binance struct {
	BaseAsset       string          `toml:"base_asset"`
	QuoteAsset      string          `toml:"quote_asset"`
	Timeout         config.Duration `toml:"timeout"`
	Log             telegraf.Logger `toml:"-"`
	tags            map[string]string
	client          *http.Client
	priceURL        *url.URL
	exchangeInfoURL *url.URL
}

// SampleConfig returns the sample configuration for the plugin.
func (*Binance) SampleConfig() string {
	return sampleConfig
}

// Init can be implemented to do one-time processing stuff like initializing variables.
func (b *Binance) Init() error {
	b.Log.Trace("Initializing Btc plugin")

	b.Log.Trace("Validating configuration")
	if b.BaseAsset == "" || b.QuoteAsset == "" {
		return errors.New("base_asset and quote_asset cannot be empty")
	}
	b.Log.AddAttribute("symbol", b.BaseAsset+b.QuoteAsset)

	b.tags = map[string]string{
		"base":  b.BaseAsset,
		"quote": b.QuoteAsset,
	}

	var (
		query string = fmt.Sprintf("symbol=%s%s", b.BaseAsset, b.QuoteAsset)
		err   error
		r     *http.Request
	)

	b.Log.Trace("Creating URLs")
	b.priceURL, err = url.Parse(priceUrlString + "?" + query)
	if err != nil {
		return fmt.Errorf("failed to parse url %s: %w", priceUrlString, err)
	}

	b.exchangeInfoURL, err = url.Parse(exchangeInfoUrlString + "?" + query)
	if err != nil {
		return fmt.Errorf("failed to parse url %s: %w", exchangeInfoUrlString, err)
	}

	b.Log.Infof("Verifying requested symbol %s", b.BaseAsset+b.QuoteAsset)

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(b.Timeout))
	defer cancel()
	if r, err = http.NewRequestWithContext(ctx, http.MethodGet, b.exchangeInfoURL.String(), nil); err != nil {
		return fmt.Errorf("failed to create request for %s: %w", b.exchangeInfoURL.String(), err)
	}
	r.Header = header

	resp, err := b.client.Do(r)
	if err != nil {
		return fmt.Errorf("failed to get response from %s: %w", b.exchangeInfoURL.String(), err)
	} else if resp.StatusCode != http.StatusOK {
		p := new(payload)
		if err := json.NewDecoder(resp.Body).Decode(p); err != nil {
			return fmt.Errorf("cannot decode response from %s: %w", b.exchangeInfoURL.String(), err)
		}
		return fmt.Errorf("binance responsed with status %s (code %d) for symbol %s", p.Msg, p.Code, b.BaseAsset+b.QuoteAsset)
	}
	b.Log.Info("plugin initialized successfully")
	return nil
}

func (b *Binance) Gather(acc telegraf.Accumulator) error {

	var (
		err    error
		r      *http.Request
		resp   *http.Response
		cancel context.CancelFunc
	)

	if r, cancel, err = b.createRequest(); err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	defer cancel()

	if resp, err = b.client.Do(r); err != nil {
		acc.AddError(fmt.Errorf("failed to get response from %s: %w", b.priceURL.String(), err))
		return nil
	} else if resp.StatusCode != http.StatusOK {
		p := new(payload)
		if err = json.NewDecoder(resp.Body).Decode(p); err != nil {
			return fmt.Errorf("cannot decode response from %s: %w", b.priceURL.String(), err)
		}
		acc.AddError(fmt.Errorf("binance responsed with status %s (code %d) for symbol %s", p.Msg, p.Code, b.BaseAsset+b.QuoteAsset))
	}

	fields := make(map[string]interface{})
	t := new(tick)
	if err = json.NewDecoder(resp.Body).Decode(t); err != nil {
		acc.AddError(fmt.Errorf("cannot decode response from %s: %w", b.priceURL.String(), err))
		return nil
	}

	fields["price"], err = strconv.ParseFloat(strings.TrimSpace(t.Price), 64)
	if err != nil {
		acc.AddError(fmt.Errorf("cannot parse price %s: %w", t.Price, err))
	}

	acc.AddFields("binance", fields, b.tags)
	return nil
}

func (b *Binance) createRequest() (*http.Request, context.CancelFunc, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(b.Timeout))
	r, _ := http.NewRequestWithContext(ctx, http.MethodGet, b.priceURL.String(), nil)
	r.Header = header
	return r, cancel, nil
}

func init() {
	inputs.Add("binance", func() telegraf.Input {
		return &Binance{
			client: &http.Client{Timeout: 5 * time.Second},
			// Set the default timeout here to distinguish it from the user setting it to zero
			Timeout: config.Duration(5 * time.Second),
		}
	})
}
