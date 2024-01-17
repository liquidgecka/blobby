package secretloader

import (
	"context"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/crewjam/saml"
	"github.com/crewjam/saml/samlsp"

	"github.com/iterable/blobby/internal/logging"
)

// Loads SAML secrets from the secret sources and provides it as an
// interface to callers. This will automatically update the provider
// if the underlying certificate changes so that the caller does not
// need to be aware of the update.
type SAMLProvider struct {
	// The URL of the Entity MetaData resources.
	IDPMetaDataURL string

	// The IDP URL that is used to direct a caller to a login page
	// if needed.
	IDPURL string

	// The URL of the MetaData server for this SAML Service Provider. This
	// is the URL that will be used to fetch MetaData resources to manage
	// the IDP -> SP relationship.
	MetaDataURL string

	// The URL of the ACS resource for this SAML provider.
	ACSURL string

	// The URL of the SLO resource for this SAML resource.
	SLOURL string

	// SAML Provider is a wrapper around the Certificate implementation
	// as it uses a certificate's public and private key for SAML
	// configuration.
	Certificate

	// A local cache of the currently active ServiceProvider as well as a
	// lock that should get held while reading and writing the data.
	serviceProvider     *saml.ServiceProvider
	serviceProviderLock sync.Mutex
}

// Makes an authentication request that can be used against the SAML
// provider in order to start the authentication process.
func (s *SAMLProvider) AuthenticationRequest() (*saml.AuthnRequest, error) {
	sp, err := s.provider()
	if err != nil {
		return nil, err
	}
	return sp.MakeAuthenticationRequest(s.IDPURL)
}

// Returns the Meta Data object for this ServicePRovider.
func (s *SAMLProvider) MetaData() (*saml.EntityDescriptor, error) {
	sp, err := s.provider()
	if err != nil {
		return nil, err
	}
	return sp.Metadata(), nil
}

// Parses a HTTP response received after the user returns from the SAML
// Identity Provider to ensure that it is valid. This requires a "id" string
// that was generated when the request to authenticate was started and
// a http.Request object that can be used for form reading.
func (s *SAMLProvider) ParseResponse(
	r *http.Request, id string,
) (
	*saml.Assertion, error,
) {
	sp, err := s.provider()
	if err != nil {
		return nil, err
	}
	ids := []string{id}
	if atn, err := sp.ParseResponse(r, ids); err != nil {
		if ire, ok := err.(*saml.InvalidResponseError); ok {
			return nil, fmt.Errorf("Error message: %q\nPrivateErr: %q\nResponse: %q\n", ire.Error(), ire.PrivateErr, ire.Response) // FIXME
		}
		return nil, err
	} else {
		return atn, nil
	}
}

// If the SAML Provider uses a secret URL that is configured to preload
// then this will automatically load the SAML Provider, otherwise this
// does nothing and returns nil.
func (s *SAMLProvider) PreLoad() error {
	switch {
	case s == nil:
		return nil
	case s.Certificate.Certificate.PreLoad():
		return s.load()
	case s.Certificate.Private.PreLoad():
		return s.load()
	default:
		return nil
	}
}

// Starts a background goroutine that will automatically refresh the
// SAML ServiceProvider as configured by the parameters in the secret
// loaders used. This will continue to run until the provided channel is
// closed.
func (s *SAMLProvider) StartRefresher(stop <-chan struct{}) {
	switch {
	case s == nil:
	case s.Certificate.Certificate != nil && s.Certificate.Certificate.Stale():
	case s.Certificate.Private != nil && s.Certificate.Private.Stale():
	default:
		dur := s.Certificate.Certificate.CacheDuration()
		if dur2 := s.Certificate.Private.CacheDuration(); dur2 < dur {
			dur = dur2
		}
		if dur > 0 {
			go s.refresher(dur, stop)
		}
	}
}

// Returns the currently active saml.ServiceProvider object. If one has not
// been initialized then this will return an error.
func (s *SAMLProvider) provider() (*saml.ServiceProvider, error) {
	s.serviceProviderLock.Lock()
	defer s.serviceProviderLock.Unlock()
	for i := 0; i < 5; i++ {
		switch {
		case s.serviceProvider == nil:
			s.load()
		case s.Certificate.Certificate.IsStale():
			s.load()
		case s.Certificate.Private.IsStale():
			s.load()
		default:
			return s.serviceProvider, nil
		}
	}
	return nil, fmt.Errorf("Unable to obtain the Service Provider.")
}

// Loads the provider from the data in the secrets.
func (s *SAMLProvider) load() error {
	wg := sync.WaitGroup{}
	var certRaw, keyRaw []byte
	var certErr, keyErr error

	// Fetch the certificate and the private key in go routines and wait
	// for both to finish before checking the results.
	wg.Add(2)
	go func() {
		defer wg.Done()
		certRaw, certErr = s.Certificate.Certificate.Fetch()
	}()
	go func() {
		defer wg.Done()
		keyRaw, keyErr = s.Certificate.Private.Fetch()
	}()
	wg.Wait()
	if certErr != nil {
		return certErr
	} else if keyErr != nil {
		return keyErr
	}

	// Parse the bytes into a certificate.
	cert, err := tls.X509KeyPair(certRaw, keyRaw)
	if err != nil {
		return fmt.Errorf(
			"Error loading certificate from '%s'/'%s': %s'",
			s.Certificate.Certificate.URL(),
			s.Certificate.Private.URL(),
			err.Error())
	}

	// The SAML library requires the private key to be RSA, even though the
	// x509 library can parse many different key types, as such we need
	// to verify that the key is RSA, otherwise this is an error.
	rsaPrivateKey, ok := cert.PrivateKey.(*rsa.PrivateKey)
	if !ok {
		return fmt.Errorf(
			"The SAML Private key needs to be an RSA key.")
	}

	// Now we need to parse out the intermediates that were read from the
	// PEM data above. These will be necessary for use with the ServiceProvider
	// later. tls returns them as raw bytes so we need to go through each one
	// parsing it and adding it to a list.
	var intermediates []*x509.Certificate
	for count, rawCerts := range cert.Certificate {
		for len(rawCerts) > 0 {
			var block *pem.Block
			block, rawCerts = pem.Decode(rawCerts)
			if block == nil {
				break
			}
			if block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
				continue
			}
			if cert, err := x509.ParseCertificate(block.Bytes); err != nil {
				// Note that the +2 here is because we loaded the certificate
				// chain from the file, the first of which is the leaf
				// certificate and is therefor not included in this parsing
				// stage here. We want the error to show the item that had
				// the actual error so we remove the zero index (+1) and ignore
				// the leaf node (+1), hence count+1.
				return fmt.Errorf(""+
					"Error loading intermediate certificate %d from (%s): %s",
					count+2,
					s.Certificate.Certificate.URL(),
					err.Error())
			} else {
				intermediates = append(intermediates, cert)
			}
		}
	}

	// Fetch the updated MetaData from the Identity Provider.
	u, err := url.Parse(s.IDPMetaDataURL)
	if err != nil {
		return fmt.Errorf(
			"Error parsing IDP Meta Data URL (%s): %s",
			s.IDPMetaDataURL,
			err.Error())
	}
	ed, err := samlsp.FetchMetadata(
		context.Background(),
		http.DefaultClient,
		*u)
	if err != nil {
		return fmt.Errorf(
			"Error fetching Meta Data from the identify provider (%s): %s",
			s.IDPMetaDataURL,
			err.Error())
	}

	// Create the service provider object with the data we collected so far.
	sp := &saml.ServiceProvider{
		Certificate:   cert.Leaf,
		EntityID:      ed.EntityID,
		IDPMetadata:   ed,
		Intermediates: intermediates,
		Key:           rsaPrivateKey,
	}

	// Parse and generate the URLs used for the SAML identity provider
	// to access this service provider.
	if u, err := url.Parse(s.ACSURL); err != nil {
		s.Logger.Error(
			"Invalid ACS URL",
			logging.NewField("acs_url", s.ACSURL),
			logging.NewFieldIface("error", err))
		return fmt.Errorf(
			"The configured ACS URL (%s) is invalid: %s",
			s.ACSURL,
			err.Error())
	} else {
		sp.AcsURL = *u
	}
	if u, err := url.Parse(s.MetaDataURL); err != nil {
		s.Logger.Error(
			"Invalid Meta Data URL",
			logging.NewField("meta_data_url", s.MetaDataURL),
			logging.NewFieldIface("error", err))
		return fmt.Errorf(
			"The configured Meta Data URL (%s) is invalid: %s",
			s.MetaDataURL,
			err.Error())
	} else {
		sp.MetadataURL = *u
	}
	if u, err := url.Parse(s.SLOURL); err != nil {
		s.Logger.Error(
			"Invalid SLO URL",
			logging.NewField("slo_url", s.SLOURL),
			logging.NewFieldIface("error", err))
		return fmt.Errorf(
			"The configured SLO URL (%s) is invalid: %s",
			s.SLOURL,
			err.Error())
	} else {
		sp.SloURL = *u
	}

	// Success
	s.serviceProvider = sp
	return nil
}

// Reloads the secret on an interval until the stop channel is closed. This
// is expected to be run as a goroutine.
func (s *SAMLProvider) refresher(dur time.Duration, stop <-chan struct{}) {
	timer := time.NewTimer(dur)
	defer func() {
		if !timer.Stop() {
			<-timer.C
		}
	}()
	for {
		select {
		case <-stop:
			return
		case <-timer.C:
		}
		s.Certificate.Logger.Debug("Refreshing the certificate data.")
		if err := s.load(); err != nil {
			s.Certificate.Logger.Error(
				"Error refreshing the certificate data.",
				logging.NewFieldIface("error", err))
		}
	}
}
