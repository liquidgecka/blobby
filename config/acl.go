package config

import (
	"fmt"
	"net"

	"github.com/iterable/blobby/httpserver/access"
	"github.com/iterable/blobby/httpserver/secretloader"
)

var (
	defaultBasicAuthRealm    = "Blobby"
	defaultBasicAuthRequired = false
	defaultSAMLRequired      = false
	defaultWebUsers          = false
	defaultWebUsersRequired  = false
	defaultWhiteListRequired = false
)

type acl struct {
	// If defined then Basic Auth will be enabled for this resource. The
	// secret will be loaded via the URL given and must match the htpassword
	// format (with additional fields allowed as tags for the user.) Those
	// tags can then match tags provided here as a require element.
	BasicAuthHTPasswdURL *string  `toml:"basic_auth_htpasswd_url"`
	BasicAuthRealm       *string  `toml:"basic_auth_realm"`
	BasicAuthUserTags    []string `toml:"basic_auth_user_tags"`

	// If Baseic Auth is required then all requests must contain correct
	// basic authentication headers. If its not required then requests are
	// required to match at least one authentication method.
	BasicAuthRequired *bool `toml:"basic_auth_required"`

	// Sets a list of SAML authentication sources that are allowed to provide
	// authentication for this resource.
	SAMLProviders []string `toml:"saml_providers"`
	SAMLUsersTags []string `toml:"saml_users_tags"`

	// If SAML authentication is required then it must be used along side of
	// all other required authentication methods. If its not required
	// then it will be allowed with any other non required authentication
	// methods.
	SAMLRequired *bool `toml:"saml_required"`

	// Set to true to allow web login users to access this resource, as well
	// as a set of tags that must be applied to those users in order to
	// access this resource.
	WebUsers     *bool    `toml:"web_users"`
	WebUsersTags []string `toml:"web_users_tags"`

	// Require web auth. Normally a user providing credentials of any form
	// are allowed. Making one required will require the user to use that
	// auth for every request.
	WebUsersRequired *bool `toml:"web_users_required"`

	// A list of restricted IP/CIDR restrictions that will be applied.
	// Ex: ["127.0.0.1/32", "10.0.0.0/24"]
	WhiteListCIDRs []string `toml:"white_list_cidrs"`

	// For white listing the configuration can specify a list of upstream
	// IP addresses that are allowed to set a X-Forwarded-For header in
	// order to allow proxying. This can be used with load balancers, or
	// with Blobby when it forwards a GET request to another server to
	// fetch local data.
	AllowXForwardedForFrom []string `toml:"allow_x_fowarded_from_cidrs"`

	// If this is set to true then the CIDR list provided above will be
	// required. If not required then having a source IP that matches a
	// CIDR in the white_list will be considered enough to be authenticated
	// and will skip further authentication steps.
	WhiteListRequired *bool `toml:"white_list_required"`

	// A link back to the top of the configuration tree.
	top *top

	// When WhiteListCIDRs is parsed the results are stored in this list.
	cidrs []net.IPNet

	// When AllowXForwardedForFrom is parsed the results are stored in this
	// list.
	allowXForwardedForFrom []net.IPNet

	// The secret loader used for htpasswd files.
	basicAuth *secretloader.HTPasswd
}

func (a *acl) access() *access.ACL {
	if a == nil {
		return nil
	}
	acl := &access.ACL{}
	keep := false

	// WhiteList
	if a.cidrs != nil {
		w := &access.WhiteList{
			AllowXForwardedForFrom: a.allowXForwardedForFrom,
			CIDRs:                  a.cidrs,
		}
		if *a.WhiteListRequired {
			acl.Required = append(acl.Required, w)
		} else {
			acl.Any = append(acl.Any, w)
		}
		keep = true
	}

	// SAML
	if len(a.SAMLProviders) > 0 {
		for _, providerStr := range a.SAMLProviders {
			s := &access.SAMLAuth{
				Source:   a.top.SAML[providerStr].Provider(),
				UserTags: a.SAMLUsersTags,
			}
			if *a.SAMLRequired {
				acl.Required = append(acl.Required, s)
			} else {
				acl.Any = append(acl.Required, s)
			}
		}
		keep = true
	}

	// WebUsers
	if a.WebUsers != nil && *a.WebUsers == true {
		w := &access.WebAuth{
			Provider: a.top.Server.WebAuthProvider(),
			UserTags: a.WebUsersTags,
		}
		if *a.WebUsersRequired {
			acl.Required = append(acl.Required, w)
		} else {
			acl.Any = append(acl.Any, w)
		}
		keep = true
	}

	// BasicAuth
	if a.basicAuth != nil {
		b := &access.BasicAuth{
			Realm:    *a.BasicAuthRealm,
			Users:    a.basicAuth,
			UserTags: a.BasicAuthUserTags,
		}
		if *a.BasicAuthRequired {
			acl.Required = append(acl.Required, b)
		} else {
			acl.Any = append(acl.Any, b)
		}
		keep = true
	}

	// Only return something if there was something defined.
	if keep {
		return acl
	} else {
		return nil
	}
}

func (a *acl) initLogging() {
	if a.basicAuth != nil {
		a.basicAuth.Logger = a.top.Log.logger.
			NewChild().
			AddField("component", "htpasswd-loader").
			AddField("url", *a.BasicAuthHTPasswdURL)
	}
}

func (a *acl) preLoad() error {
	if a != nil && a.basicAuth != nil {
		if err := a.basicAuth.PreLoad(); err != nil {
			return err
		}
	}
	return nil
}

func (a *acl) startRefresher(stop <-chan struct{}) {
	if a != nil {
		if a.basicAuth != nil {
			a.basicAuth.StartRefresher(stop)
		}
	}
}

func (a *acl) validate(top *top, name string) []string {
	var errors []string
	a.top = top

	// BasicAuthHTPasswdURL
	if a.BasicAuthHTPasswdURL != nil {
		l, err := secretloader.NewLoader(
			*a.BasicAuthHTPasswdURL,
			a.top.getProfiles())
		if err != nil {
			errors = append(errors, fmt.Sprintf(
				"%s.basic_auth_htpasswd_url: invalid url: %s",
				name,
				err.Error()))
		} else {
			a.basicAuth = &secretloader.HTPasswd{
				Source: l,
			}
		}
	}

	// BasicAuthRealm
	if a.BasicAuthRealm == nil {
		a.BasicAuthRealm = &defaultBasicAuthRealm
	} else if *a.BasicAuthRealm == "" {
		errors = append(
			errors,
			fmt.Sprintf("%s.basic_auth_realm can not be empty", name))
	}

	// BasicAuthUserTags
	errors = append(
		errors,
		hasDuplicates(name+".basic_auth_user_tags", a.BasicAuthUserTags)...)
	errors = append(
		errors,
		hasEmpty(name+".basic_auth_user_tags", a.BasicAuthUserTags)...)

	// BasicAuthRequired
	switch {
	case a.BasicAuthRequired == nil:
		a.BasicAuthRequired = &defaultBasicAuthRequired
	case !*a.BasicAuthRequired:
	case a.BasicAuthHTPasswdURL == nil:
		errors = append(errors, fmt.Sprintf(
			"%s.basic_auth_required is true but no password url was defined.",
			name))
	}

	// SAMLProviders
	for _, provider := range a.SAMLProviders {
		_, ok := a.top.SAML[provider]
		if !ok {
			errors = append(
				errors,
				fmt.Sprintf(
					"%s.saml_providers: %s is not a valid provider.",
					name,
					provider))
		}
	}

	// SAMLUsersTags
	errors = append(
		errors,
		hasDuplicates(name+".saml_user_tags", a.SAMLUsersTags)...)
	errors = append(
		errors,
		hasEmpty(name+".saml_user_tags", a.SAMLUsersTags)...)

	// SAMLRequired
	switch {
	case a.SAMLRequired == nil:
		a.SAMLRequired = &defaultSAMLRequired
	case !*a.SAMLRequired:
	case len(a.SAMLProviders) == 0:
		errors = append(errors, fmt.Sprintf(
			"%s.saml_required is true but no providers were configured.",
			name))
	}

	// WebUsers
	if a.WebUsers == nil {
		a.WebUsers = &defaultWebUsers
	} else if a.top.Server.WebUsersHTPasswdURL == nil {
		errors = append(errors, fmt.Sprintf(
			"%s.web_users requires server.web_users_htpasswd_url be defined.",
			name))
	}

	// WebUsersTags
	errors = append(
		errors,
		hasDuplicates(name+".web_user_tags", a.WebUsersTags)...)
	errors = append(
		errors,
		hasEmpty(name+".web_user_tags", a.WebUsersTags)...)

	// WebUsersRequired
	if a.WebUsersRequired == nil {
		a.WebUsersRequired = &defaultWebUsersRequired
	}

	// WhiteListCIDRs
	for _, cidr := range a.WhiteListCIDRs {
		_, ipnet, err := net.ParseCIDR(cidr)
		if err != nil {
			errors = append(
				errors,
				fmt.Sprintf(
					"%s.white_list_cidrs: Invalid CIDR '%s': %s",
					name,
					cidr,
					err.Error(),
				))
		} else {
			a.cidrs = append(a.cidrs, *ipnet)
		}
	}

	// AllowXForwardedForFrom
	for _, cidr := range a.AllowXForwardedForFrom {
		_, ipnet, err := net.ParseCIDR(cidr)
		if err != nil {
			errors = append(
				errors,
				fmt.Sprintf(
					"%s.allow_x_fowarded_from_cidrs: Invalid CIDR '%s': %s",
					name,
					cidr,
					err.Error(),
				))
		} else {
			a.allowXForwardedForFrom = append(a.allowXForwardedForFrom, *ipnet)
		}
	}

	// WhiteListRequired
	switch {
	case a.WhiteListRequired == nil:
		a.WhiteListRequired = &defaultWhiteListRequired
	case !*a.WhiteListRequired:
	case len(a.WhiteListCIDRs) == 0:
		errors = append(errors, fmt.Sprintf(""+
			"%s.white_list_required is true, but no white list was defined.",
			name))
	}

	// Return any errors encountered.
	return errors
}
