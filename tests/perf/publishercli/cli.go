package publishercli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/url"
	"strconv"
	"strings"

	"github.com/posthog/duckgres/tests/perf/publisher"
)

type Dependencies struct {
	Stdin   io.Reader
	Stdout  io.Writer
	Publish func(context.Context, publisher.Config, string) error
}

type connectionSecret struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Database string `json:"database"`
	Username string `json:"username"`
	Password string `json:"password"`
}

func Run(ctx context.Context, args []string, deps Dependencies) error {
	stdin := deps.Stdin
	if stdin == nil {
		stdin = strings.NewReader("")
	}
	stdout := deps.Stdout
	if stdout == nil {
		stdout = io.Discard
	}
	publish := deps.Publish
	if publish == nil {
		publish = publisher.PublishRunDir
	}

	flags := flag.NewFlagSet("duckgres-perf-publisher", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	runDir := flags.String("run-dir", "", "directory containing summary.json and query_results.csv")
	dsn := flags.String("dsn", "", "publisher PGWire DSN")
	password := flags.String("password", "", "optional password override for --dsn")
	secretStdin := flags.Bool("connection-secret-stdin", false, "read host, port, database, username, and password JSON from stdin")
	schema := flags.String("schema", "", "target schema (publisher default when empty)")
	bootstrap := flags.Bool("bootstrap-schema", true, "create the target schema and tables when missing")
	if err := flags.Parse(args); err != nil {
		return fmt.Errorf("parse flags: %w", err)
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments: %s", strings.Join(flags.Args(), " "))
	}
	if strings.TrimSpace(*runDir) == "" {
		return errors.New("run-dir is required")
	}
	if (strings.TrimSpace(*dsn) == "") == !*secretStdin {
		return errors.New("exactly one of --dsn and --connection-secret-stdin is required")
	}

	cfg := publisher.Config{
		DSN:             strings.TrimSpace(*dsn),
		Password:        *password,
		Schema:          strings.TrimSpace(*schema),
		BootstrapSchema: *bootstrap,
	}
	if *secretStdin {
		secret, err := decodeConnectionSecret(stdin)
		if err != nil {
			return err
		}
		cfg.DSN = secret.dsn()
		cfg.Password = secret.Password
	}

	if err := publish(ctx, cfg, *runDir); err != nil {
		return fmt.Errorf("publish perf artifacts: %w", err)
	}
	_, _ = fmt.Fprintf(stdout, "published perf artifacts from %s\n", *runDir)
	return nil
}

func decodeConnectionSecret(r io.Reader) (connectionSecret, error) {
	var secret connectionSecret
	decoder := json.NewDecoder(r)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&secret); err != nil {
		return connectionSecret{}, fmt.Errorf("decode connection secret: %w", err)
	}
	required := []struct {
		name  string
		value string
	}{
		{name: "host", value: secret.Host},
		{name: "database", value: secret.Database},
		{name: "username", value: secret.Username},
		{name: "password", value: secret.Password},
	}
	for _, field := range required {
		if strings.TrimSpace(field.value) == "" {
			return connectionSecret{}, fmt.Errorf("connection secret is missing %s", field.name)
		}
	}
	if secret.Port < 1 || secret.Port > 65535 {
		return connectionSecret{}, errors.New("connection secret port must be between 1 and 65535")
	}
	return secret, nil
}

func (s connectionSecret) dsn() string {
	query := url.Values{
		"connect_timeout": {"15"},
		"sslmode":         {"require"},
	}
	return (&url.URL{
		Scheme:   "postgresql",
		User:     url.User(s.Username),
		Host:     net.JoinHostPort(s.Host, strconv.Itoa(s.Port)),
		Path:     "/" + s.Database,
		RawQuery: query.Encode(),
	}).String()
}
