package core

import (
	"fmt"
	"strings"
)

type IntentMatcher struct{}

func NewIntentMatcher() *IntentMatcher {
	return &IntentMatcher{}
}

func (m *IntentMatcher) SQLFor(query Query, protocol Protocol) (string, error) {
	switch protocol {
	case ProtocolPGWire:
		if strings.TrimSpace(query.PGWireSQL) == "" {
			return "", fmt.Errorf("query %s missing pgwire_sql", query.QueryID)
		}
		return query.PGWireSQL, nil
	case ProtocolFlight:
		if strings.TrimSpace(query.DuckhogSQL) == "" {
			return "", fmt.Errorf("query %s missing duckhog_sql", query.QueryID)
		}
		return query.DuckhogSQL, nil
	default:
		return "", fmt.Errorf("unknown protocol %q", protocol)
	}
}
