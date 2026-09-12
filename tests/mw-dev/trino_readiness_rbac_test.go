package e2emwdev_test

import (
	"io"
	"os"
	"slices"
	"strings"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
)

func TestTrinoSecretReadinessPermissions(t *testing.T) {
	for _, fixture := range []struct {
		file      string
		role      string
		namespace string
	}{
		{"manifests.tmpl.yaml", "duckgres-control-plane", "suite-test"},
		{"trino-multicell.tmpl.yaml", "trino-cell-projection", "cell-test"},
	} {
		t.Run(fixture.role, func(t *testing.T) {
			raw, err := os.ReadFile(fixture.file)
			if err != nil {
				t.Fatal(err)
			}
			rendered := os.Expand(string(raw), func(name string) string {
				switch name {
				case "NAMESPACE":
					return "suite-test"
				case "TRINO_CELL_NAMESPACE":
					return "cell-test"
				default:
					return "fixture-value"
				}
			})
			decoder := utilyaml.NewYAMLOrJSONDecoder(strings.NewReader(rendered), 4096)
			var rules []rbacv1.PolicyRule
			bound := false
			for {
				var object struct {
					Kind     string              `json:"kind"`
					Metadata metav1.ObjectMeta   `json:"metadata"`
					Rules    []rbacv1.PolicyRule `json:"rules"`
					Subjects []rbacv1.Subject    `json:"subjects"`
					RoleRef  rbacv1.RoleRef      `json:"roleRef"`
				}
				if err := decoder.Decode(&object); err == io.EOF {
					break
				} else if err != nil {
					t.Fatal(err)
				}
				if object.Metadata.Namespace != fixture.namespace {
					continue
				}
				if object.Kind == "Role" && object.Metadata.Name == fixture.role {
					rules = object.Rules
				}
				if object.Kind == "RoleBinding" && object.RoleRef.Kind == "Role" && object.RoleRef.Name == fixture.role {
					for _, subject := range object.Subjects {
						if subject.Kind == "ServiceAccount" && subject.Name == "duckgres" && subject.Namespace == "suite-test" {
							bound = true
						}
					}
				}
			}
			if !bound {
				t.Fatal("readiness permissions must be bound to the control-plane ServiceAccount")
			}
			for _, request := range []struct{ resource, verb string }{
				{"pods", "get"}, {"pods", "list"}, {"pods/exec", "create"},
			} {
				allowed := false
				for _, rule := range rules {
					if slices.Contains(rule.APIGroups, "") && slices.Contains(rule.Resources, request.resource) && slices.Contains(rule.Verbs, request.verb) && len(rule.ResourceNames) == 0 {
						allowed = true
					}
				}
				if !allowed {
					t.Errorf("control plane cannot %s %s in %s to verify mounted Trino credentials", request.verb, request.resource, fixture.namespace)
				}
			}
			for _, rule := range rules {
				if slices.Contains(rule.Resources, "pods/exec") && (!slices.Equal(rule.Verbs, []string{"create"}) || !slices.Equal(rule.APIGroups, []string{""}) || !slices.Equal(rule.Resources, []string{"pods/exec"})) {
					t.Errorf("pod exec grant must be limited to creating exec requests: %+v", rule)
				}
			}
		})
	}
}
