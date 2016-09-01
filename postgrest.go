package postgrest

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"time"
)

// errors
var (
	errMissingConfigParams  = errors.New("postgrest error: missing config parameter")
	errMissingHTTPClient    = errors.New("postgrest error: missing httpClient parameter")
	errMissingJWTGenerator  = errors.New("postgrest error: missing jwtGenerator parameter")
	errMissingRequestMethod = errors.New("postgrest error: missing request method")
	errMissingRequestURL    = errors.New("postgrest error: missing request url")
	errMissingURLPath       = errors.New("postgrest error: table name not specified in request")
	errMissingRoleClaim     = errors.New("postgrest error: missing 'role' in postgrest claims")
	errInvalidExpiryClaim   = errors.New("postgrest error: invalid 'exp' in postgrest claims")
)

// Config contains config data for making postgREST calls
type Config struct {
	Issuer        string        `yaml:"issuer,omitempty"`
	MasterBaseURL string        `yaml:"master_base_url" required:"true"`
	MasterRole    string        `yaml:"master_role" required:"true"`
	MasterSecret  string        `yaml:"master_secret" required:"true"`
	SlaveBaseURL  string        `yaml:"slave_base_url" required:"true"`
	SlaveRole     string        `yaml:"slave_role" required:"true"`
	SlaveSecret   string        `yaml:"slave_secret" required:"true"`
	Timeout       time.Duration `yaml:"timeout" required:"true"`
}

// isSuccess returns true if the http status code is inclusively between 200 and 300
func isSuccess(httpStatusCode int) bool {
	return httpStatusCode >= 200 && httpStatusCode < 300
}

// jsonEncode converts the given payload into an io.Reader interface and produces JSON as bytes
func jsonEncode(payload interface{}) (io.Reader, error) {
	b := new(bytes.Buffer)
	err := json.NewEncoder(b).Encode(payload)
	return b, err
}

// unmarshalResponse unmarshals the body of an http.Response object into the target interface
func unmarshalResponse(response *http.Response, target interface{}) (int, error) {
	defer response.Body.Close()

	if !isSuccess(response.StatusCode) {
		return response.StatusCode, fmt.Errorf("postgrest error (%s %s): %s", response.Request.Method, response.Request.URL.String(), response.Status)
	}
	if target == nil {
		return response.StatusCode, nil
	}

	if err := json.NewDecoder(response.Body).Decode(target); err != nil {
		return http.StatusInternalServerError, err
	}
	return response.StatusCode, nil
}

// generateClaims generates jwt claims for the given role
func generateClaims(role string, config *Config) *Claims {
	return &Claims{
		Role:      role,
		Issuer:    config.Issuer,
		ExpiresAt: time.Now().Add(config.Timeout * time.Second).Unix(),
	}
}

func newRequest(method, urlStr, tokenStr string, body io.Reader) (*http.Request, error) {
	request, err := http.NewRequest(method, urlStr, body)
	if err != nil {
		return nil, err
	}
	request.Header.Add("Authorization", tokenStr)
	return request, nil
}

// buildURL return a *url.URL object from the given `baseURL`, `path` and `queryParams`
func buildURLStr(baseURL, path string, queryParams *url.Values) (string, error) {
	if path == "" {
		return "", errMissingURLPath
	}
	url, err := url.Parse(baseURL)
	if err != nil {
		return "", err
	}
	var encodedQuery string
	if queryParams != nil {
		encodedQuery = queryParams.Encode()
	}
	url.Path = path
	url.RawQuery = encodedQuery

	return url.String(), nil
}

// validateConfig ensures that all required data is available in Config
func validateConfig(config *Config) error {
	var invalidFields []string

	valueData := reflect.ValueOf(config).Elem()
	typeData := valueData.Type()

	for i := 0; i < valueData.NumField(); i++ {
		fieldType := typeData.Field(i)
		fieldValue := valueData.Field(i)

		if required, ok := fieldType.Tag.Lookup("required"); !ok || required != "true" {
			continue
		}

		if fieldValue.Type().String() == "string" && fieldValue.String() == "" {
			invalidFields = append(invalidFields, fieldType.Name)
		}

		if fieldValue.Type().String() == "time.Duration" && fieldValue.Int() <= 0 {
			invalidFields = append(invalidFields, fieldType.Name)
		}
	}

	if invalidFields != nil {
		return fmt.Errorf("postgrest error: invalid config parameters: \n- %s",
			strings.Join(invalidFields, "\n- "))
	}

	return nil
}

// Claims contains data necessary to make a postgREST claims
type Claims struct {
	Role      string `json:"role,omitempty"`
	Issuer    string `json:"iss,omitempty"`
	ExpiresAt int64  `json:"exp,omitempty"`
}

// Valid ensures that the given claims are valid
func (c Claims) Valid() error {
	if c.Role == "" {
		return errMissingRoleClaim
	}
	if c.ExpiresAt <= time.Now().Unix() {
		return errInvalidExpiryClaim
	}
	return nil
}

// HTTPClientAdapter is an interface for sending http.Request objects
type HTTPClientAdapter interface {
	Do(*http.Request) (*http.Response, error)
}

// PgrestAdapter is an interface that describes the pgrestAgent
type PgrestAdapter interface {
	Delete(table string, query *url.Values) (*http.Response, error)
	DeleteJSON(table string, query *url.Values) (int, error)
	Get(table string, query *url.Values) (*http.Response, error)
	GetJSON(table string, query *url.Values, target interface{}) (int, error)
	NewRequest(method, urlStr string, body io.Reader) (*http.Request, error)
	Patch(table string, query *url.Values, body io.Reader) (*http.Response, error)
	PatchJSON(table string, query *url.Values, payload interface{}) (int, error)
	Ping() error
	Post(table string, body io.Reader) (*http.Response, error)
	PostAndReturn(table string, body io.Reader) (*http.Response, error)
	PostJSON(table string, payload interface{}, target interface{}) (int, error)
}

// JWTGenerator is an interface for generating JSON Web Tokens
type JWTGenerator func(claims interface{}, secret string) (tokenStr string, err error)

// Agent encapsulates methods for making HTTP requests to a postgREST service
type Agent struct {
	config      *Config
	httpClient  HTTPClientAdapter
	generateJWT JWTGenerator
	PgrestAdapter
}

// NewRequest generates a new request with authorization header for postgrest service
func (agent *Agent) NewRequest(method, urlStr string, body io.Reader) (*http.Request, error) {
	if urlStr == "" {
		return nil, errMissingRequestURL
	}
	if method == "" {
		return nil, errMissingRequestMethod
	}
	if method == http.MethodGet {
		return agent.newReadRequest(method, urlStr)
	}
	return agent.newWriteRequest(method, urlStr, body)
}

func (agent *Agent) newReadRequest(method, urlStr string) (*http.Request, error) {
	tokenStr, err := agent.generateReadTokenStr()
	if err != nil {
		return nil, err
	}
	return newRequest(method, urlStr, tokenStr, nil)
}

func (agent *Agent) newWriteRequest(method, urlStr string, body io.Reader) (*http.Request, error) {
	tokenStr, err := agent.generateWriteTokenStr()
	if err != nil {
		return nil, err
	}
	return newRequest(method, urlStr, tokenStr, body)
}

// generateAuthTokenStr generates an authentication string for an Postgrest HTTP authorization header
func (agent *Agent) generateReadTokenStr() (string, error) {
	claims := generateClaims(agent.config.SlaveRole, agent.config)
	tokenStr, err := agent.generateJWT(claims, agent.config.SlaveSecret)
	return fmt.Sprintf("Bearer %s", tokenStr), err
}

// generateAuthTokenStr generates an authentication string for an Postgrest HTTP authorization header
func (agent *Agent) generateWriteTokenStr() (string, error) {
	claims := generateClaims(agent.config.MasterRole, agent.config)
	tokenStr, err := agent.generateJWT(claims, agent.config.MasterSecret)
	return fmt.Sprintf("Bearer %s", tokenStr), err
}

// Ping sends a request to the postgrest master and slave servers
// and returns an error if the response status is not bwtween 200 and 299
func (agent *Agent) Ping() error {
	var urls = []struct {
		name string
		url  string
	}{
		{"master", agent.config.MasterBaseURL},
		{"slave", agent.config.SlaveBaseURL},
	}
	for _, url := range urls {
		request, err := agent.sendRequest(http.MethodGet, url.url, nil)
		if err != nil {
			return fmt.Errorf("%s service error: %v", url.name, err)
		}
		if !isSuccess(request.StatusCode) {
			return fmt.Errorf("%s service error: %v", url.name, errors.New(request.Status))
		}
	}
	return nil
}

// sendRequest sends an HTTP request using the httpClient
func (agent *Agent) sendRequest(method, urlStr string, body io.Reader) (*http.Response, error) {
	request, err := agent.NewRequest(method, urlStr, body)
	if err != nil {
		return nil, err
	}
	return agent.httpClient.Do(request)
}

// Get makes an HTTP GET request to the postgREST slave service specified in the config.
// To paginate response, set the `offset` and `limit` parameters in the `query` e.g:
// query.Set("limit", 10)
// query.Set("offset", 10)
func (agent *Agent) Get(table string, query *url.Values) (*http.Response, error) {
	urlStr, err := buildURLStr(agent.config.SlaveBaseURL, table, query)
	if err != nil {
		return nil, err
	}
	return agent.sendRequest(http.MethodGet, urlStr, nil)
}

// GetJSON makes an HTTP GET request to a postgREST service and unmarshals
// the response into the given target interface
// Returns error if response status code is not inclusively between 200 and 299
func (agent *Agent) GetJSON(table string, query *url.Values, target interface{}) (int, error) {
	response, err := agent.Get(table, query)
	if err != nil {
		return 0, err
	}
	return unmarshalResponse(response, target)
}

// Post makes an HTTP POST request to the postgREST master service specified in the config.
func (agent *Agent) Post(table string, body io.Reader) (*http.Response, error) {
	urlStr, err := buildURLStr(agent.config.MasterBaseURL, table, nil)
	if err != nil {
		return nil, err
	}
	return agent.sendRequest(http.MethodPost, urlStr, body)
}

// PostJSON makes an HTTP POST request to a postgREST service and unmarshals
// the response into the given target interface
// Returns error if the response status code is not inclusively between 200 and 299
func (agent *Agent) PostJSON(table string, payload interface{}, target interface{}) (int, error) {
	var response *http.Response
	body, err := jsonEncode(payload)
	if err != nil {
		return 0, err
	}
	if target == nil {
		response, err = agent.Post(table, body)
		if err != nil {
			return 0, err
		}
		return unmarshalResponse(response, nil)
	}
	response, err = agent.PostAndReturn(table, body)
	if err != nil {
		return 0, err
	}
	return unmarshalResponse(response, target)
}

// PostAndReturn makes an HTTP POST request to the postgREST master service specified in the config
// and returns the http.Response with a representation of the posted object.
func (agent *Agent) PostAndReturn(table string, body io.Reader) (*http.Response, error) {
	urlStr, err := buildURLStr(agent.config.MasterBaseURL, table, nil)
	if err != nil {
		return nil, err
	}
	request, err := agent.NewRequest(http.MethodPost, urlStr, body)
	if err != nil {
		return nil, err
	}
	request.Header.Add("Prefer", "return=representation")
	return agent.httpClient.Do(request)
}

// Patch makes an HTTP PATCH request to a postgREST service specified in the config
func (agent *Agent) Patch(table string, query *url.Values, body io.Reader) (*http.Response, error) {
	urlStr, err := buildURLStr(agent.config.MasterBaseURL, table, query)
	if err != nil {
		return nil, err
	}
	return agent.sendRequest(http.MethodPatch, urlStr, body)
}

// PatchJSON makes an HTTP PATCH request to a postgREST service
// Returns an error if the response status code is not inclusively between 200 and 299
func (agent *Agent) PatchJSON(table string, query *url.Values, payload interface{}) (int, error) {
	body, err := jsonEncode(payload)
	if err != nil {
		return 0, err
	}
	response, err := agent.Patch(table, query, body)
	if err != nil {
		return 0, err
	}
	return unmarshalResponse(response, nil)
}

// Delete makes an HTTP DELETE request to the postgREST master service specified in the config
func (agent *Agent) Delete(table string, query *url.Values) (*http.Response, error) {
	urlStr, err := buildURLStr(agent.config.MasterBaseURL, table, query)
	if err != nil {
		return nil, err
	}
	return agent.sendRequest(http.MethodDelete, urlStr, nil)
}

// DeleteJSON makes an HTTP DELETE request to a postgREST service
// Returns an error if the response status code is not inclusively between 200 and 299
func (agent *Agent) DeleteJSON(table string, query *url.Values) (int, error) {
	response, err := agent.Delete(table, query)
	if err != nil {
		return 0, err
	}
	return unmarshalResponse(response, nil)
}

// NewAgent returns a new instance of Agent
func NewAgent(config *Config, httpClient HTTPClientAdapter, jwtGenerator JWTGenerator) (*Agent, error) {
	if config == nil {
		return nil, errMissingConfigParams
	}
	if httpClient == nil {
		return nil, errMissingHTTPClient
	}
	if jwtGenerator == nil {
		return nil, errMissingJWTGenerator
	}
	err := validateConfig(config)
	if err != nil {
		return nil, err
	}
	return &Agent{config: config, httpClient: httpClient, generateJWT: jwtGenerator}, nil
}
