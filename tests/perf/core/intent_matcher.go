package core

type IntentMatcher struct{}

func NewIntentMatcher() *IntentMatcher {
	return &IntentMatcher{}
}

func (m *IntentMatcher) SQLFor(query Query, protocol Protocol) (string, error) {
	return query.SQLFor(protocol)
}
