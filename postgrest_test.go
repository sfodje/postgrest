package postgrest

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

var (
	server     *httptest.Server
	testConfig *Config
	testAgent  *Agent
	testTable  = "test_table"
)

func TestMain(m *testing.M) {
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, testTable) && r.URL.Path != "/" {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, http.StatusText(http.StatusNotFound))
			return
		}
		if code, _ := strconv.Atoi(r.URL.Query().Get("error")); code > 0 {
			w.WriteHeader(code)
			fmt.Fprint(w, http.StatusText(code))
			return
		}

		switch r.Method {
		case http.MethodGet:
			objectBytes, _ := json.Marshal(testObject)
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, string(objectBytes))
			return
		case http.MethodDelete:
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, http.StatusText(http.StatusOK))
			return
		case http.MethodPost:
			w.WriteHeader(http.StatusCreated)
			if r.Header.Get("Prefer") == "return=representation" {
				objectBytes, _ := json.Marshal(testObject)
				fmt.Fprint(w, string(objectBytes))
				return
			}
			fmt.Fprint(w, http.StatusText(http.StatusCreated))
			return
		case http.MethodPatch:
			w.WriteHeader(http.StatusNoContent)
			fmt.Fprint(w, http.StatusText(http.StatusNoContent))
			return
		default:
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(w, http.StatusText(http.StatusInternalServerError))
			return
		}
	}))
	defer server.Close()
	testConfig = &Config{
		Issuer:        "test",
		MasterBaseURL: server.URL,
		MasterRole:    "masterRole",
		MasterSecret:  "masterSecret",
		SlaveBaseURL:  server.URL,
		SlaveRole:     "slaveRole",
		SlaveSecret:   "slaveSecret",
		Timeout:       5,
	}
	testAgent = &Agent{
		config:      testConfig,
		httpClient:  &http.Client{},
		generateJWT: func(_ interface{}, _ string) (string, error) { return "secret", nil },
	}

	os.Exit(m.Run())
}

func TestNewAgent(t *testing.T) {
	t.Parallel()

	_, err := NewAgent(nil, nil, nil)
	if err == nil || err.Error() != errMissingConfigParams.Error() {
		t.Errorf("NewAgent returned unexpected error:\nExpected: %v\nGot: %v", errMissingConfigParams, err)
	}

	_, err = NewAgent(testConfig, nil, nil)
	if err == nil || err.Error() != errMissingHTTPClient.Error() {
		t.Errorf("NewAgent returned unexpected error:\nExpected: %v\nGot: %v", errMissingHTTPClient, err)
	}

	_, err = NewAgent(testConfig, &http.Client{}, nil)
	if err == nil || err.Error() != errMissingJWTGenerator.Error() {
		t.Errorf("NewAgent returned unexpected error:\nExpected: %v\nGot: %v", errMissingJWTGenerator, err)
	}

	_, err = NewAgent(&Config{}, &http.Client{}, func(_ interface{}, _ string) (string, error) { return "", nil })
	if err == nil || !strings.Contains(err.Error(), "invalid config parameters") {
		t.Errorf("NewAgent returned unexpected error:\nExpected: %v...\nGot: %v", errMissingConfigParams, err)
	}

	agent, err := NewAgent(testConfig, &http.Client{}, func(_ interface{}, _ string) (string, error) { return "", nil })
	if err != nil {
		fmt.Println(testConfig)
		t.Errorf("NewAgent returned unexpected error: %v", err)
	}
	if agent == nil {
		t.Error("NewAgent did not return agent as expected")
	}
}

func TestGet(t *testing.T) {
	t.Parallel()

	query := &url.Values{}
	query.Set("id", fmt.Sprintf("%d", testObject.ID))

	response, err := testAgent.Get(testTable, query)
	if err != nil {
		t.Errorf("Get returned unexpected error: %v", err)
	}
	if response.StatusCode != http.StatusOK {
		t.Errorf("Get returned unexpected status code:\nExpected: %d\nGot: %d", http.StatusOK, response.StatusCode)
	}

	response, err = testAgent.Get("tableNoExist", query)
	if err != nil {
		t.Errorf("Get returned unexpected error: %v", err)
	}
	if response.StatusCode != http.StatusNotFound {
		t.Errorf("Get returned unexpected status code:\nExpected: %d\nGot: %d", http.StatusNotFound, response.StatusCode)
	}

	response, err = testAgent.Get(testTable, query)
	if err != nil {
		t.Errorf("Get returned unexpected error: %v", err)
	}
	if response.StatusCode != http.StatusOK {
		t.Errorf("Get returned unexpected status code:\nExpected: %d\nGot: %d", http.StatusOK, response.StatusCode)
	}

	agent := &Agent{}
	*agent = *testAgent
	agent.config.SlaveBaseURL = "://xy/"
	expectedError := errors.New("parse ://xy/: missing protocol scheme")
	_, err = agent.Get(testTable, query)
	if err == nil || err.Error() != expectedError.Error() {
		t.Errorf("Get returned unexpected error:\nExpected: %v\nGot: %v", expectedError, err)
	}
}

func TestGetJSON(t *testing.T) {
	t.Parallel()

	query := &url.Values{}
	query.Set("id", fmt.Sprintf("%d", testObject.ID))
	agent := &Agent{}
	*agent = *testAgent
	agent.generateJWT = func(_ interface{}, _ string) (string, error) {
		return "", errors.New("mock error")
	}

	expectedError := errors.New("mock error")
	status, err := agent.GetJSON(testTable, query, nil)
	if err == nil || err.Error() != expectedError.Error() {
		t.Errorf("GetJSON returned an unexpected error:\nExpected: %v\n%d\nGot: %v", expectedError, status, err)
	}

	obj := &object{}
	status, err = testAgent.GetJSON(testTable, query, obj)
	if err != nil {
		t.Errorf("GetJSON returned an unexpected error: %v", err)
	}
	if status != http.StatusOK {
		t.Errorf("GetJSON returned an unexpected status code:\nExpected: %d\nGot: %d", http.StatusOK, status)
	}
	if !reflect.DeepEqual(obj, testObject) {
		t.Errorf("GetJSON returned unexpected results:\nExpected: %v\nGot: %v", testObject, obj)
	}
}

func TestPost(t *testing.T) {
	t.Parallel()

	bodyBytes, _ := json.Marshal(testObject)
	agent := &Agent{}
	*agent = *testAgent
	config := &Config{}
	*config = *testConfig
	config.MasterBaseURL = "://xy/"
	agent.config = config
	expectedError := errors.New("parse ://xy/: missing protocol scheme")
	_, err := agent.Post(testTable, bytes.NewBuffer(bodyBytes))
	if err == nil || err.Error() != expectedError.Error() {
		t.Errorf("Post returned unexpected error:\nExpected: %v\nGot: %v", expectedError, err)
	}

	expectedError = errors.New("Post test_table: unsupported protocol scheme \"\"")
	config.MasterBaseURL = ""
	_, err = agent.Post(testTable, bytes.NewBuffer(bodyBytes))
	if err == nil || err.Error() != expectedError.Error() {
		t.Errorf("Post returned unexpected error:\nExpected: %v\nGot: %v", expectedError, err)
	}

	response, err := testAgent.Post(testTable, bytes.NewBuffer(bodyBytes))
	if err != nil {
		t.Errorf("Post returned unexpected error: %v", err)
	}
	if response.StatusCode != http.StatusCreated {
		t.Errorf("Post returned unexpected status code:\nExpected: %d\nGot: %d", http.StatusCreated, response.StatusCode)
	}
}

func TestPostJSON(t *testing.T) {
	t.Parallel()

	bodyBytes, _ := json.Marshal(testObject)
	status, err := testAgent.PostJSON(testTable, bytes.NewBuffer(bodyBytes), nil)
	if err != nil {
		t.Errorf("PostJSON returned unexpected error: %v", err)
	}
	if status != http.StatusCreated {
		t.Errorf("PostJSON returned unexpected status code:\nExpected: %d\nGot: %d", http.StatusCreated, status)
	}

	agent := &Agent{}
	*agent = *testAgent
	config := &Config{}
	*config = *testConfig
	config.MasterBaseURL = "://xy/"
	agent.config = config
	expectedError := errors.New("parse ://xy/: missing protocol scheme")
	_, err = agent.PostJSON(testTable, bytes.NewBuffer(bodyBytes), nil)
	if err == nil || err.Error() != expectedError.Error() {
		t.Errorf("PostJSON returned unexpected error:\nExpected: %v\nGot: %v", expectedError, err)
	}

	obj := &object{}
	expectedError = errors.New("parse ://xy/: missing protocol scheme")
	_, err = agent.PostJSON(testTable, bytes.NewBuffer(bodyBytes), obj)
	if err == nil || err.Error() != expectedError.Error() {
		t.Errorf("PostJSON returned unexpected error:\nExpected: %v\nGot: %v", expectedError, err)
	}

	obj = &object{}
	status, err = testAgent.PostJSON(testTable, bytes.NewBuffer(bodyBytes), obj)
	if err != nil {
		t.Errorf("PostJSON returned an unexpected error: %v", err)
	}
	if status != http.StatusCreated {
		t.Errorf("PostJSON returned an unexpected status code:\nExpected: %d\nGot: %d", http.StatusCreated, status)
	}
	if !reflect.DeepEqual(obj, testObject) {
		t.Errorf("PostJSON returned unexpected results:\nExpected: %v\nGot: %v", testObject, obj)
	}

	obj = &object{}
	*agent = *testAgent
	*config = *testConfig
	expectedError = errors.New("mock error")
	agent.generateJWT = func(_ interface{}, _ string) (string, error) { return "", expectedError }
	_, err = agent.PostJSON(testTable, bytes.NewBuffer(bodyBytes), obj)
	if err == nil || err.Error() != expectedError.Error() {
		t.Errorf("PostJSON returned unexpected error:\nExpected: %v\nGot: %v", expectedError, err)
	}
}

func TestPatch(t *testing.T) {
	t.Parallel()

	query := &url.Values{}
	query.Set("id", fmt.Sprintf("%d", testObject.ID))
	bodyBytes, _ := json.Marshal(testObject)
	agent := &Agent{}
	*agent = *testAgent
	config := &Config{}
	*config = *testConfig
	config.MasterBaseURL = "://xy/"
	agent.config = config
	expectedError := errors.New("parse ://xy/: missing protocol scheme")
	_, err := agent.Patch(testTable, query, bytes.NewBuffer(bodyBytes))
	if err == nil || err.Error() != expectedError.Error() {
		t.Errorf("Patch returned unexpected error:\nExpected: %v\nGot: %v", expectedError, err)
	}

	expectedError = errors.New("unsupported protocol scheme \"\"")
	config.MasterBaseURL = ""
	_, err = agent.Patch(testTable, query, bytes.NewBuffer(bodyBytes))
	if err == nil || !strings.Contains(err.Error(), expectedError.Error()) {
		t.Errorf("Patch returned unexpected error:\nExpected: %v\nGot: %v", expectedError, err)
	}

	response, err := testAgent.Patch(testTable, query, bytes.NewBuffer(bodyBytes))
	if err != nil {
		t.Errorf("Patch returned unexpected error: %v", err)
	}
	if response.StatusCode != http.StatusNoContent {
		t.Errorf("Patch returned unexpected status code:\nExpected: %d\nGot: %d", http.StatusNoContent, response.StatusCode)
	}
}

func TestPatchJSON(t *testing.T) {
	t.Parallel()

	query := &url.Values{}
	query.Set("id", fmt.Sprintf("%d", testObject.ID))
	bodyBytes, _ := json.Marshal(testObject)
	status, err := testAgent.PatchJSON(testTable, query, bytes.NewBuffer(bodyBytes))
	if err != nil {
		t.Errorf("PatchJSON returned unexpected error: %v", err)
	}
	if status != http.StatusNoContent {
		t.Errorf("PatchJSON returned unexpected status code:\nExpected: %d\nGot: %d", http.StatusNoContent, status)
	}

	agent := &Agent{}
	*agent = *testAgent
	config := &Config{}
	*config = *testConfig
	config.MasterBaseURL = "://xy/"
	agent.config = config
	expectedError := errors.New("parse ://xy/: missing protocol scheme")
	_, err = agent.PatchJSON(testTable, query, bytes.NewBuffer(bodyBytes))
	if err == nil || err.Error() != expectedError.Error() {
		t.Errorf("PatchJSON returned unexpected error:\nExpected: %v\nGot: %v", expectedError, err)
	}

	expectedError = errors.New("parse ://xy/: missing protocol scheme")
	_, err = agent.PatchJSON(testTable, query, bytes.NewBuffer(bodyBytes))
	if err == nil || err.Error() != expectedError.Error() {
		t.Errorf("PatchJSON returned unexpected error:\nExpected: %v\nGot: %v", expectedError, err)
	}

	status, err = testAgent.PatchJSON(testTable, query, bytes.NewBuffer(bodyBytes))
	if err != nil {
		t.Errorf("PatchJSON returned an unexpected error: %v", err)
	}
	if status != http.StatusNoContent {
		t.Errorf("PatchJSON returned an unexpected status code:\nExpected: %d\nGot: %d", http.StatusNoContent, status)
	}
}

func TestDelete(t *testing.T) {
	t.Parallel()

	query := &url.Values{}
	query.Set("id", fmt.Sprintf("%d", testObject.ID))
	response, err := testAgent.Delete(testTable, query)
	if err != nil {
		t.Errorf("Delete returned an unexpected error:%v", err)
	}
	if response.StatusCode != http.StatusOK {
		t.Errorf("Delete returned an unexpected status code:\nExpected: %d\nGot: %d", http.StatusOK, response.StatusCode)
	}

	agent := &Agent{}
	*agent = *testAgent
	config := &Config{}
	*config = *testConfig
	config.MasterBaseURL = "://xy/"
	agent.config = config
	expectedError := errors.New("parse ://xy/: missing protocol scheme")
	response, err = agent.Delete(testTable, query)
	if err != nil && err.Error() != expectedError.Error() {
		t.Errorf("Delete returned unexpected error:\nExpected: %v\nGot: %v", expectedError, err)
	}
	if err == nil {
		t.Errorf("Delete returned unexpected error:\nExpected: %v\nGot: %v", expectedError, err)
	}
}

func TestDeleteJSON(t *testing.T) {
	t.Parallel()

	query := &url.Values{}
	query.Set("id", fmt.Sprintf("%d", testObject.ID))
	status, err := testAgent.DeleteJSON(testTable, query)
	if err != nil {
		t.Errorf("Delete returned an unexpected error:%v", err)
	}
	if status != http.StatusOK {
		t.Errorf("Delete returned an unexpected status code:\nExpected: %d\nGot: %d", http.StatusOK, status)
	}

	agent := &Agent{}
	*agent = *testAgent
	config := &Config{}
	*config = *testConfig
	config.MasterBaseURL = "://xy/"
	agent.config = config
	expectedError := errors.New("parse ://xy/: missing protocol scheme")
	_, err = agent.DeleteJSON(testTable, query)
	if err == nil || err.Error() != expectedError.Error() {
		t.Errorf("Delete returned unexpected error:\nExpected: %v\nGot: %v", expectedError, err)
	}
}

func TestPing(t *testing.T) {
	t.Parallel()
	if err := testAgent.Ping(); err != nil {
		t.Errorf("Ping returned unexpected error: %v", err)
	}

	agent := &Agent{}
	*agent = *testAgent
	config := &Config{}
	*config = *testConfig
	config.MasterBaseURL = "://xy/"
	agent.config = config
	expectedError := errors.New("master service error: parse ://xy/: missing protocol scheme")
	if err := agent.Ping(); err == nil || err.Error() != expectedError.Error() {
		t.Errorf("Ping returned unexpected error:\nExpected: %v\nGot: %v", expectedError, err)
	}

	config.MasterBaseURL = server.URL + "?error=404"
	expectedError = errors.New("master service error: 404 Not Found")
	if err := agent.Ping(); err == nil || err.Error() != expectedError.Error() {
		t.Errorf("Ping returned unexpected error:\nExpected: %v\nGot: %v", expectedError, err)
	}
}

func TestNewRequest(t *testing.T) {
	t.Parallel()

	_, err := testAgent.NewRequest("", "", nil)
	expectedError := errMissingRequestURL
	if err == nil || err.Error() != expectedError.Error() {
		t.Errorf("NewRequest returned unexpected error:\nExpected: %v\nGot: %v", expectedError, err)
	}

	_, err = testAgent.NewRequest("", "sdfasdfa", nil)
	expectedError = errMissingRequestMethod
	if err == nil || err.Error() != expectedError.Error() {
		t.Errorf("NewRequest returned unexpected error:\nExpected: %v\nGot: %v", expectedError, err)
	}
}

func TestClaims(t *testing.T) {
	t.Parallel()

	claims := &Claims{
		Role:      "role",
		Issuer:    "issuer",
		ExpiresAt: time.Now().Add(time.Hour * 1).Unix(),
	}
	if err := claims.Valid(); err != nil {
		t.Errorf("Claims returned unexpected error: %v", err)
	}

	claims.Role = ""
	expectedError := errMissingRoleClaim
	if err := claims.Valid(); err == nil || err.Error() != expectedError.Error() {
		t.Errorf("Claims returned unexpected error:\nExpected: %v\nGot: %v", expectedError, err)
	}

	claims.Role = "role"
	claims.ExpiresAt = time.Now().Unix()
	expectedError = errInvalidExpiryClaim
	if err := claims.Valid(); err == nil || err.Error() != expectedError.Error() {
		t.Errorf("Claims returned unexpected error:\nExpected: %v\nGot: %v", expectedError, err)
	}
}

func TestMisc(t *testing.T) {
	t.Parallel()

	request := &http.Request{URL: &url.URL{}}
	response := &http.Response{Request: request, StatusCode: http.StatusNotFound, Body: ioutil.NopCloser(bytes.NewBuffer([]byte("test")))}
	_, err := unmarshalResponse(response, nil)
	expectedError := errors.New("postgrest error ( ): ")
	if err == nil || err.Error() != expectedError.Error() {
		t.Errorf("unmarshalResponse returned unexpected error:\nExpected: %v\nGot: %v", expectedError, err)
	}

	response.StatusCode = http.StatusOK
	response.Request = httptest.NewRequest(http.MethodGet, server.URL, nil)
	_, err = unmarshalResponse(response, func() {})
	expectedError = errors.New("invalid character 'e' in literal true (expecting 'r')")
	if err == nil || err.Error() != expectedError.Error() {
		t.Errorf("unmarshalResponse returned unexpected error:\nExpected: %v\nGot: %v", expectedError, err)
	}

	_, err = buildURLStr("", "", nil)
	expectedError = errMissingURLPath
	if err == nil || err.Error() != expectedError.Error() {
		t.Errorf("buildURLStr returned unexpected error:\nExpected: %v\nGot: %v", expectedError, err)
	}
}

// **************** mocks, structs, funcs and interfaces **************** //

type object struct {
	ID          int    `json:"id"`
	FirstName   string `json:"first_name"`
	LastName    string `json:"last_name"`
	Email       string `json:"email"`
	PhoneNumber string `json:"phone_number"`
}

var testObject = &object{12345678900, "Test", "Testerson", "ttesterson@tester.test", "(000)000-0000"}
