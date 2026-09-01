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
	case ProtocolPGWire, ProtocolTrino:
		if strings.TrimSpace(query.CanonicalSQL()) == "" {
			return "", fmt.Errorf("query %s missing canonical SQL", query.QueryID)
		}
		return query.CanonicalSQL(), nil
	default:
		return "", fmt.Errorf("unknown protocol %q", protocol)
	}
}
