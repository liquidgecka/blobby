package config

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/arn"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/ec2rolecreds"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
)

// Each proxy is configured via this definition.
type AWS struct {
	KeyID           *string `toml:"key_id"`
	Region          *string `toml:"region"`
	SecretKey       *string `toml:"secret_key"`
	AssumeRoleARN   *string `toml:"assume_role_arn"`
	Profile         *string `toml:"profile"`
	FromEnvironment *bool   `toml:"from_environment"`
	FromEC2Role     *bool   `toml:"from_ec2_role"`

	// The name given to this AWS profile.
	name string `toml:"-"`

	// Stores the actual AWS session that was initialized using the credentials
	// and configuration provided here.
	session *session.Session `toml:"-"`
}

// Populates the Session() variable and returns it.
func (a *AWS) GetSession() *session.Session {
	return a.session

}

// Validate the contents of the AWS object.
func (a *AWS) validate(name string) []string {
	var errors []string
	switch {
	case a.KeyID == nil && a.SecretKey == nil:
	case a.KeyID != nil && a.SecretKey != nil:
	default:
		errors = append(errors, fmt.Sprintf(
			"aws.%s.key_id and aws.%s.secret_key must be used together.",
			name,
			name,
		))
	}
	if a.KeyID != nil && *a.KeyID == "" {
		errors = append(errors, fmt.Sprintf(
			"aws.%s.key_id can not be an empty string.",
			name,
		))
	}
	if a.SecretKey != nil && *a.SecretKey == "" {
		errors = append(errors, fmt.Sprintf(
			"aws.%s.secret_key can not be an empty string.",
			name))
	}
	if a.Region != nil && *a.Region == "" {
		errors = append(errors, fmt.Sprintf(
			"aws.%s.region can not be an empty string.",
			name,
		))
	}
	if a.AssumeRoleARN != nil {
		if *a.AssumeRoleARN == "" {
			errors = append(errors, fmt.Sprintf(
				"aws.%s.assume_role_arn can not be an empty string.",
				name,
			))
		} else if parsed, err := arn.Parse(*a.AssumeRoleARN); err != nil {
			errors = append(errors, fmt.Sprintf(
				"aws.%s.assume_role_arn is not a valid arn: %s",
				name,
				err.Error(),
			))
		} else if parsed.Service != "iam" {
			errors = append(errors, fmt.Sprintf(
				"aws.%s.assume_role_arn is not an iam ARN (it is %s instead)",
				name,
				parsed.Service,
			))
		}
	}

	// Count the methods that are supposed to be used to auth. If its not
	// exactly 1 then report an error.
	authMethods := make([]string, 0, 4)
	if a.KeyID != nil {
		authMethods = append(authMethods, "provided key")
	}
	if a.FromEnvironment != nil && *a.FromEnvironment {
		authMethods = append(authMethods, "from environment")
	}
	if a.FromEC2Role != nil && *a.FromEC2Role {
		authMethods = append(authMethods, "from ec2 role")
	}
	if a.Profile != nil {
		authMethods = append(authMethods, "from profile")
	}
	if len(authMethods) > 1 {
		errors = append(errors, fmt.Sprintf(
			"aws.%s: More than one AWS authentication method selected.",
			name,
		))
	}

	// If there were no errors setting up the AWS client then this will
	// initialize the session for use by other parts of the configuration
	// process.
	for errors == nil {
		// Setup the Options object that will be used to initialize the
		// session.
		opts := session.Options{
			Config: aws.Config{Region: a.Region},
		}

		// Based on the credential configuration we need to initialize the
		// Credentials field.
		if a.KeyID != nil {
			opts.Config.Credentials = credentials.NewStaticCredentials(
				*a.KeyID,
				*a.SecretKey,
				"",
			)
		} else if a.FromEnvironment != nil && *a.FromEnvironment {
			opts.Config.Credentials = credentials.NewEnvCredentials()
		} else if a.Profile != nil {
			opts.Profile = *a.Profile
		} else if a.FromEC2Role != nil && *a.FromEC2Role {
			opts.Config.Credentials = ec2rolecreds.NewCredentials(
				session.New(aws.NewConfig()),
			)
		}

		// Attempt to initialize the session.
		sess, err := session.NewSessionWithOptions(opts)
		if err != nil {
			errors = append(errors, fmt.Sprintf(
				"aws.%s: Error initializing the AWS session: %s",
				name,
				err.Error()))
			break
		}

		// If this session is going to assume a role then we need to initialize
		// that portion of the client first so the second pass will initialize
		// a session as the right role.
		if len(errors) == 0 && a.AssumeRoleARN != nil {
			creds := stscreds.NewCredentials(sess, *a.AssumeRoleARN)
			sess, err = session.NewSession(&aws.Config{
				Region:      a.Region,
				Credentials: creds,
			})
			if err != nil {
				errors = append(errors, fmt.Sprintf(
					"aws.%s: Error assuming rule '%q': %s",
					name,
					*a.AssumeRoleARN,
					err.Error()))
				break
			}
		}

		// Finalize.
		a.session = sess
		break
	}

	// Success
	return errors
}

// A very simple implementation of httpserver/certloader.Sessions
type profiles map[string]*session.Session

func (p profiles) CheckProfile(n string) bool {
	_, ok := p[n]
	return ok
}

func (p profiles) GetSession(n string) *session.Session {
	return p[n]
}
