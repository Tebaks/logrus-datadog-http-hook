package hook

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/sirupsen/logrus"
)

type DataDogOptions struct {
	// DataDog API key to authenticate
	APIKey string
	// Minimum log level at which to send logs to DataDog
	MinLevel logrus.Level
	// Base URL of the DataDog API
	BaseURL string
	// Base Path of the DataDog API
	BasePath string
	// Service name to send to DataDog
	Service string
	// Source name to send to DataDog
	Source string
	// The host tag to send to DataDog
	Host string
}

type DataDogHook struct {
	APIKey    string
	MinLevel  logrus.Level
	URL       *url.URL
	Service   string
	Host      string
	Formatter logrus.Formatter
}

const (
	apiKeyHeader = "DD-API-KEY"
	contentType  = "application/json"
	// Maximum content size for a single log: 256kb
	maxEntryByte = 256 * 1024
	maxRetry     = 3
)

var (
	ErrMissingAPIKey = errors.New("missing DataDog API key")

	defaultMinLevel = logrus.InfoLevel
	defaultBaseURL  = "http://http-intake.logs.datadoghq.eu"
	defaultBasePath = "/v1/input"
)

func NewDataDogHook(options DataDogOptions) (*DataDogHook, error) {
	if options.APIKey == "" {
		return nil, ErrMissingAPIKey
	}

	setDefaults(&options)
	url, err := buildURL(options.BaseURL, options.BasePath, options.Service, options.Source, options.Host)
	if err != nil {
		return nil, err
	}

	hook := &DataDogHook{
		APIKey:    options.APIKey,
		MinLevel:  options.MinLevel,
		URL:       url,
		Service:   options.Service,
		Host:      options.Host,
		Formatter: &logrus.JSONFormatter{},
	}

	return hook, nil
}

func (dh *DataDogHook) Levels() []logrus.Level {
	return logrus.AllLevels[:dh.MinLevel+1]
}

func (dh *DataDogHook) Fire(entry *logrus.Entry) error {
	log, err := dh.Formatter.Format(entry)
	if err != nil {
		return err
	}

	return dh.send(log)
}

func (dh *DataDogHook) send(log []byte) error {
	if len(log) > maxEntryByte {
		log = log[:maxEntryByte]
	}

	i := 0
	for {
		req, err := http.NewRequest("POST", dh.URL.String(), bytes.NewBuffer(log))
		if err != nil {
			return err
		}

		req.Header.Add(apiKeyHeader, dh.APIKey)
		req.Header.Add("Content-Type", contentType)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}
		if i >= maxRetry {
			body, _ := ioutil.ReadAll(resp.Body)
			return fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, body)
		}
		i++
		time.Sleep(time.Second)
	}
}

func buildURL(baseURL, basePath, service, source, host string) (*url.URL, error) {
	url, err := url.Parse(baseURL + basePath)
	if err != nil {
		return url, err
	}

	params := url.Query()
	params.Add("service", service)
	params.Add("ddsource", "go")
	params.Add("host", host)

	url.RawQuery = params.Encode()

	return url, nil
}

func setDefaults(options *DataDogOptions) {
	if options.MinLevel == 0 {
		options.MinLevel = defaultMinLevel
	}
	if options.BaseURL == "" {
		options.BaseURL = defaultBaseURL
	}
	if options.BasePath == "" {
		options.BasePath = defaultBasePath
	}
}
