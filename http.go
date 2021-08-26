/*
Copyright © 2021 Dirk Lembke

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package nifi

import (
	"bytes"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"

	"golang.org/x/crypto/pkcs12"
)

type HttpClient struct {
	client *http.Client
}

func NewHttpCertClient(file string, password string, ca string, insecureSkipVerify bool) (*HttpClient, error) {
	rc := &HttpClient{}

	client, err := rc.createClient(file, password, ca, insecureSkipVerify)
	if err != nil {
		return nil, err
	}

	rc.client = client
	return rc, nil
}

func NewHttpClient(ca string, insecureSkipVerify bool) (*HttpClient, error) {
	rc := &HttpClient{}
	var pool *x509.CertPool

	if ca != "" {
		pool = x509.NewCertPool()

		certs, err := rc.LoadPemCertificate(ca)
		if err != nil {
			return nil, err
		}

		for _, cert := range certs {
			pool.AddCert(cert)
		}
	}

	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			RootCAs:            pool,
			InsecureSkipVerify: insecureSkipVerify,
		},
	}
	rc.client = &http.Client{
		Transport: transport,
	}

	return rc, nil
}

func (c *HttpClient) createClient(file string, password string, ca string, insecureSkipVerify bool) (*http.Client, error) {
	cert, pool, err := c.ReadPKCS12(file, password)
	if err != nil {
		return nil, err
	}

	if ca != "" {
		certs, err := c.LoadPemCertificate(ca)
		if err != nil {
			return nil, err
		}

		for _, cert := range certs {
			pool.AddCert(cert)
		}
	}

	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			RootCAs:            pool,
			Certificates:       []tls.Certificate{*cert},
			InsecureSkipVerify: insecureSkipVerify,
		},
	}
	return &http.Client{
		Transport: transport,
	}, nil
}

func (c *HttpClient) ReadPKCS12(file string, password string) (*tls.Certificate, *x509.CertPool, error) {
	b, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, nil, err
	}

	pemBlock, err := pkcs12.ToPEM(b, password)
	if err != nil {
		return nil, nil, err
	}

	pool := x509.NewCertPool()
	var tlsCert *tls.Certificate
	var privateKey crypto.PrivateKey

	for _, p := range pemBlock {

		switch p.Type {
		case "PRIVATE KEY":
			if privateKey, err = x509.ParsePKCS1PrivateKey(p.Bytes); err != nil {
				if privateKey, err = x509.ParsePKCS8PrivateKey(p.Bytes); err != nil {
					return nil, nil, err
				}
			}

		case "CERTIFICATE":
			cert, err := x509.ParseCertificate(p.Bytes)
			if err != nil {
				return nil, nil, err
			}

			if cert.IsCA {
				pool.AddCert(cert)
			} else {
				tlsCert = &tls.Certificate{
					Certificate: [][]byte{
						cert.Raw,
					},
					Leaf: cert,
				}
			}
		}
	}

	tlsCert.PrivateKey = privateKey

	if privateKey == nil {
		return nil, nil, fmt.Errorf("private key is missing")
	}

	if tlsCert == nil {
		return nil, nil, fmt.Errorf("client certificate is missing")
	}

	switch pub := tlsCert.Leaf.PublicKey.(type) {
	case *rsa.PublicKey:
		priv, ok := privateKey.(*rsa.PrivateKey)
		if !ok {
			return nil, nil, errors.New("tls: private key type does not match public key type")
		}
		if pub.N.Cmp(priv.N) != 0 {
			return nil, nil, errors.New("tls: private key does not match public key")
		}
	case *ecdsa.PublicKey:
		priv, ok := privateKey.(*ecdsa.PrivateKey)
		if !ok {
			return nil, nil, errors.New("tls: private key type does not match public key type")
		}
		if pub.X.Cmp(priv.X) != 0 || pub.Y.Cmp(priv.Y) != 0 {
			return nil, nil, errors.New("tls: private key does not match public key")
		}
	case ed25519.PublicKey:
		priv, ok := privateKey.(ed25519.PrivateKey)
		if !ok {
			return nil, nil, errors.New("tls: private key type does not match public key type")
		}
		if !bytes.Equal(priv.Public().(ed25519.PublicKey), pub) {
			return nil, nil, errors.New("tls: private key does not match public key")
		}
	default:
		return nil, nil, errors.New("tls: unknown public key algorithm")

	}

	return tlsCert, pool, nil
}

func (c *HttpClient) LoadPemCertificate(path string) ([]*x509.Certificate, error) {
	raw, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	certs := []*x509.Certificate{}

	for {
		block, rest := pem.Decode(raw)
		if block == nil {
			break
		}

		if block.Type == "CERTIFICATE" {
			cert, err := x509.ParseCertificate(block.Bytes)
			if err != nil {
				return nil, err
			}

			if cert.IsCA {
				certs = append(certs, cert)
			}
		}

		raw = rest
	}

	return certs, nil
}

func (c *HttpClient) Do(req *http.Request) (*http.Response, error) {
	return c.client.Do(req)
}
