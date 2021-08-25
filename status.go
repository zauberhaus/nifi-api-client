package nifi

import (
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"time"

	"gopkg.in/yaml.v3"

	"gopkg.in/square/go-jose.v2/jwt"
)

type Status struct {
	User  string
	Token string
	//Cookies []*http.Cookie
	Cookies  map[string]string
	Expire   time.Time
	Aud      string
	Insecure bool
	Server   string
}

type ByTime []time.Time

func (a ByTime) Len() int           { return len(a) }
func (a ByTime) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByTime) Less(i, j int) bool { return a[i].Before(a[j]) }

func NewStatus(server *url.URL, token string, cookies []*http.Cookie, insecure bool) (*Status, error) {
	var claims map[string]interface{}

	result, err := jwt.ParseSigned(token)
	if err != nil {
		return nil, err
	}

	err = result.UnsafeClaimsWithoutVerification(&claims)
	if err != nil {
		return nil, err
	}

	m := make(map[string]string)
	for _, c := range cookies {
		m[c.Name] = c.Value
	}

	status := &Status{
		Token:    token,
		Cookies:  m,
		Server:   server.String(),
		Insecure: insecure,
	}

	sub, ok := claims["sub"]
	if ok {
		val, ok := sub.(string)
		if ok {
			status.User = val
		}
	}

	exp, ok := claims["exp"]
	if ok {
		val, ok := exp.(float64)
		if ok {
			status.Expire = time.Unix(int64(val), 0)
		}
	}

	aud, ok := claims["aud"]
	if ok {
		val, ok := aud.(string)
		if ok {
			status.Aud = val
		}
	}

	return status, nil
}

func (s *Status) GetCookies() []*http.Cookie {
	rc := []*http.Cookie{}

	for k, v := range s.Cookies {
		rc = append(rc, &http.Cookie{
			Name:  k,
			Value: v,
		})
	}

	return rc
}

func (s *Status) NewRequest(method, url string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}

	if len(s.Token) > 0 {
		req.Header.Add("Authorization", "Bearer "+s.Token)
	}

	for _, c := range s.GetCookies() {
		req.AddCookie(c)
	}

	return req, err
}

func (s *Status) Load(file string) error {
	data, err := ioutil.ReadFile(file)
	if err != nil {
		return err
	}

	return yaml.Unmarshal(data, s)
}

func (s *Status) Save(file string) error {
	data, err := yaml.Marshal(s)
	if err != nil {
		return nil
	}

	return ioutil.WriteFile(file, data, os.ModePerm)
}
