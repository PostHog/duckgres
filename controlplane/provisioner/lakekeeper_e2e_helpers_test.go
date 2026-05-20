//go:build kubernetes

package provisioner

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

func ensureNamespace(t *testing.T, kc kubernetes.Interface, name string) error {
	t.Helper()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	if _, err := kc.CoreV1().Namespaces().Create(context.Background(), ns, metav1.CreateOptions{}); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return nil
		}
		return err
	}
	return nil
}

func metav1DeleteOpts() metav1.DeleteOptions {
	policy := metav1.DeletePropagationBackground
	return metav1.DeleteOptions{PropagationPolicy: &policy}
}

// dropDatabaseAndRole removes both the DB and the role created by EnsureRole.
// Used by the e2e test's t.Cleanup. Best-effort — logs but doesn't fail.
func dropDatabaseAndRole(t *testing.T, dsn, dbName string) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Logf("cleanup open: %v", err)
		return
	}
	defer db.Close()
	roleName := dbName
	_, _ = db.Exec("REASSIGN OWNED BY " + quoteIdent(roleName) + " TO CURRENT_USER")
	_, _ = db.Exec("DROP OWNED BY " + quoteIdent(roleName))
	if _, err := db.Exec("DROP DATABASE IF EXISTS " + quoteIdent(dbName)); err != nil {
		t.Logf("cleanup drop database %s: %v", dbName, err)
	}
	if _, err := db.Exec(fmt.Sprintf("DROP ROLE IF EXISTS %s", quoteIdent(roleName))); err != nil {
		t.Logf("cleanup drop role %s: %v", roleName, err)
	}
}
