# PostgREST
An HTTP client wrapper for making REST requests to a PostgREST service in Golang

## Installation
```
$ go get bitbucket.org/sfodje/postgrest
```

## Sample Usage
```go
import (
	"fmt"
	"net/http"
	"net/url"
	"time"

	jwt "github.com/dgrijalva/jwt-go"

	"bitbucket.org/sfodje/postgrest"
)

// postgrest configuration
var config = &postgrest.Config{
	Issuer:        "Iris Test",
	Timeout:       10,
	MasterBaseURL: "http://master-service.com",
	MasterRole:    "test_role",
	MasterSecret:  "test_secret",
	SlaveBaseURL:  "http://slave-service.com",
	SlaveRole:     "test_role",
	SlaveSecret:   "test_secret",
}

// application model object
type user struct {
	ID        int    `json:"id"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
	Email     string `json:"email"`
}

// required function that returns a jwt string
var jwtGenerator = func(claims interface{}, secret string) (string, error) {
	myclaims := claims.(jwt.Claims)
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, myclaims)
	return token.SignedString([]byte(secret))
}

func main() {
	// required httpClient
	httpClient := &http.Client{Timeout: config.Timeout * time.Second}
	// initialize postgrest agent
	agent, err := postgrest.NewAgent(config, httpClient, jwtGenerator)
	if err != nil {
		panic(err)
	}
	// SELECT id, first_name, last_name FROM users WHERE last_name = 'TEST'
	// GET /users?select=id,first_name,last_name&last_name=eq.TEST
	queryParams := &url.Values{}
	queryParams.Set("select", "id,first_name,last_name,email")
	queryParams.Set("last_name", "eq.TEST")
	users := &[]user{}


	err = agent.GetJSON("users", queryParams, users)
	if err != nil {
		panic(err)
	}
	for _, user := range *users {
		fmt.Printf("FirstName: %s\nLastName: %s\nEmail: %s\n\n", user.FirstName, user.LastName, user.Email)
	}
	// or
	response, err := agent.Get("users", queryParams)
	defer response.Body.Close()
	if err != nil {
	  panic(err)
	}
	if response.StatusCode == http.StatusOK {
	    data, _ := ioutil.ReadAll(response.Body)
	    //handle data ...
	}


	// INSERT INTO users (first_name, last_name) values("Tester", "McTesterson")
	// POST /users
	// payload: {"first_name": "Tester", "last_name": "McTesterson"}
	payload := bytes.NewReader([]bytes(`{"first_name": "Tester", "last_name": "McTesterson"}`))
	err = agent.PostJSON("users", payload, users)
	if err != nil {
		panic(err)
	}
	// handle users

	// or

	response, err := agent.Post("users", payload)
	// handle response ...


	// INSERT INTO users (first_name, last_name) values("Tester", "McTesterson") RETURNING *
	// POST /users
	// payload: {"first_name": "Tester", "last_name": "McTesterson"}
	// header: {Prefer: "return=representation"}
	payload := bytes.NewReader([]bytes(`{"first_name": "Tester", "last_name": "McTesterson"}`))
	response, err := agent.PostAndReturn(table, payload)
	// handle response ...
}
```

## Development
### Todo
		- Implement circuit breaker option (unless that can be handled by the http client that is passed in)
