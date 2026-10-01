package trinopool

import "errors"

// NodeReplacementEvidence identifies the first observed infrastructure replacement request.
// It never authorizes resource deletion or classifies a process as dead.
type NodeReplacementEvidence struct {
	NodeName      string `json:"node_name"`
	NodeUID       string `json:"node_uid"`
	NodeClaimName string `json:"nodeclaim_name"`
	NodeClaimUID  string `json:"nodeclaim_uid"`
	Reason        string `json:"reason"`
}

func (e NodeReplacementEvidence) Validate() error {
	if e.NodeName == "" || e.NodeUID == "" || e.NodeClaimName == "" || e.NodeClaimUID == "" {
		return errors.New("node replacement requires exact node and NodeClaim identities")
	}
	if e.Reason != "Drifted" && e.Reason != "Deleting" {
		return errors.New("node replacement requires authoritative drift or deletion evidence")
	}
	return nil
}
