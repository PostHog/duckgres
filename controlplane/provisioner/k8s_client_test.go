//go:build kubernetes

package provisioner

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func mrWithPolicies(policies []interface{}) *unstructured.Unstructured {
	spec := map[string]interface{}{}
	if policies != nil {
		spec["managementPolicies"] = policies
	}
	return &unstructured.Unstructured{Object: map[string]interface{}{"spec": spec}}
}

// TestDescribeManagementPolicies pins the op-log rendering of a managed
// resource's managementPolicies (the orphan wait's periodic diagnostic), and
// that its reading of "absent means full lifecycle" matches the conservative
// managementPoliciesHaveDelete used for the orphan decision itself.
func TestDescribeManagementPolicies(t *testing.T) {
	cases := []struct {
		name       string
		policies   []interface{}
		want       string
		haveDelete bool
	}{
		{"absent defaults to full lifecycle", nil, `absent (defaults to ["*"], full lifecycle incl. Delete)`, true},
		{"wildcard", []interface{}{"*"}, `["*"]`, true},
		{"explicit delete", []interface{}{"Observe", "Create", "Update", "Delete"}, `["Observe","Create","Update","Delete"]`, true},
		{"orphan-ready no-delete", []interface{}{"Observe", "Create", "Update"}, `["Observe","Create","Update"]`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			obj := mrWithPolicies(tc.policies)
			if got := describeManagementPolicies(obj); got != tc.want {
				t.Fatalf("describeManagementPolicies = %q, want %q", got, tc.want)
			}
			if got := managementPoliciesHaveDelete(obj); got != tc.haveDelete {
				t.Fatalf("managementPoliciesHaveDelete = %t, want %t", got, tc.haveDelete)
			}
		})
	}
}
