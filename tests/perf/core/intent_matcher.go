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
	case ProtocolPGWire, ProtocolPGWireUncached, ProtocolPGWireCached, ProtocolTrino, ProtocolAthena:
		if strings.TrimSpace(query.SQLForProtocol(protocol)) == "" {
			return "", fmt.Errorf("query %s missing canonical SQL", query.QueryID)
		}
		return query.SQLForProtocol(protocol), nil
	default:
		return "", fmt.Errorf("unknown protocol %q", protocol)
	}
}
