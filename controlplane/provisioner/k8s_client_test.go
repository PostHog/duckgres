//go:build kubernetes

package provisioner

import (
	"context"
	"strings"
	"testing"

	"github.com/posthog/duckgres/controlplane/configstore"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

func newFakeDucklingClient() (*DucklingClient, *dynamicfake.FakeDynamicClient) {
	scheme := runtime.NewScheme()
	fakeClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, map[schema.GroupVersionResource]string{
		ducklingGVR: "DucklingList",
		s3BucketGVR: "BucketList",
	})
	return NewDucklingClientWithDynamic(fakeClient), fakeClient
}

func ducklingCR(name string, status map[string]interface{}) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "k8s.posthog.com/v1alpha1",
		"kind":       "Duckling",
		"metadata": map[string]interface{}{
			"name":      name,
			"namespace": ducklingNamespace,
		},
		"status": status,
	}}
}

func readyDucklingStatus(bucketName string) map[string]interface{} {
	return map[string]interface{}{
		"metadataStore": map[string]interface{}{
			"type":     "external",
			"endpoint": "org-bucket.cluster.us-east-1.rds.amazonaws.com",
			"password": "supersecret123",
			"user":     "postgres",
			"database": "postgres",
		},
		"dataStore": map[string]interface{}{
			"type":       "s3bucket",
			"bucketName": bucketName,
			"s3Region":   "us-east-1",
		},
		"iamRoleArn": "arn:aws:iam::123456789012:role/duckling-orgbucket",
		"conditions": []interface{}{
			map[string]interface{}{"type": "Ready", "status": "True"},
			map[string]interface{}{"type": "Synced", "status": "True"},
		},
	}
}

func addS3BucketRef(status map[string]interface{}, bucketName string) {
	status["resourceRefs"] = []interface{}{
		map[string]interface{}{
			"apiVersion": s3BucketAPIVersion,
			"kind":       crossplaneBucketKind,
			"name":       bucketName,
		},
	}
}

func addCrossplaneS3BucketRef(cr *unstructured.Unstructured, bucketName string) {
	cr.Object["spec"] = map[string]interface{}{
		crossplaneSpecField: map[string]interface{}{
			crossplaneResourceRefsField: []interface{}{
				map[string]interface{}{
					"apiVersion": s3BucketAPIVersion,
					"kind":       crossplaneBucketKind,
					"name":       bucketName,
				},
			},
		},
	}
}

func bucketCR(name string, ready bool, message string) *unstructured.Unstructured {
	readyStatus := "False"
	syncedStatus := "False"
	if ready {
		readyStatus = "True"
		syncedStatus = "True"
	}
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": s3BucketAPIVersion,
		"kind":       crossplaneBucketKind,
		"metadata":   map[string]interface{}{"name": name},
		"status": map[string]interface{}{
			"conditions": []interface{}{
				map[string]interface{}{"type": "Ready", "status": readyStatus},
				map[string]interface{}{"type": "Synced", "status": syncedStatus, "message": message},
			},
		},
	}}
}

func seedReadyS3Bucket(t *testing.T, fakeK8s *dynamicfake.FakeDynamicClient, ctx context.Context, bucketName string) {
	t.Helper()
	if _, err := fakeK8s.Resource(s3BucketGVR).Create(ctx, bucketCR(bucketName, true, ""), metav1.CreateOptions{}); err != nil {
		t.Fatalf("seed ready S3 Bucket %q: %v", bucketName, err)
	}
}

func TestDucklingCreateExternalRequiresFields(t *testing.T) {
	dc, fakeK8s := newFakeDucklingClient()
	ctx := context.Background()
	if err := dc.Create(ctx, "ext-bad", CreateOptions{MetadataStoreType: configstore.MetadataStoreKindExternal}); err == nil {
		t.Fatal("expected error creating external CR without endpoint/passwordAwsSecret")
	}
	if _, getErr := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Get(ctx, ducklingName("ext-bad"), metav1.GetOptions{}); getErr == nil {
		t.Error("CR should not have been created when external validation failed")
	}
}

func TestDucklingCreateExternalDataStoreRequiresBucket(t *testing.T) {
	dc, _ := newFakeDucklingClient()
	err := dc.Create(context.Background(), "ext-nobucket", CreateOptions{
		MetadataStoreType:         configstore.MetadataStoreKindExternal,
		ExternalEndpoint:          "h",
		ExternalPasswordAWSSecret: "s",
		DataStoreType:             "external",
	})
	if err == nil {
		t.Fatal("expected error: external dataStore without a bucket name")
	}
}

func TestDucklingGetDoesNotInspectComposedBucketReadiness(t *testing.T) {
	dc, fakeK8s := newFakeDucklingClient()
	ctx := context.Background()

	status := readyDucklingStatus("posthog-duckling-org-bucket-mw-prod-us")
	addS3BucketRef(status, "posthog-duckling-org-bucket-mw-prod-us")
	if _, err := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Create(ctx, ducklingCR(ducklingName("org-bucket"), status), metav1.CreateOptions{}); err != nil {
		t.Fatalf("seed Duckling CR: %v", err)
	}

	got, err := dc.Get(ctx, "org-bucket")
	if err != nil {
		t.Fatalf("Get should only parse Duckling status: %v", err)
	}
	if got.SyncedFalseMessage != "" {
		t.Fatalf("Get should not mutate status with composed Bucket errors, got %q", got.SyncedFalseMessage)
	}
}

func TestDucklingGetProvisioningStatusReportsBucketReadinessSource(t *testing.T) {
	dc, fakeK8s := newFakeDucklingClient()
	ctx := context.Background()
	bucketName := "posthog-duckling-org-bucket-mw-prod-us"

	cr := ducklingCR(ducklingName("org-bucket"), map[string]interface{}{
		"dataStore": map[string]interface{}{
			"type":       "s3bucket",
			"bucketName": bucketName,
			"s3Region":   "us-east-1",
		},
	})
	addCrossplaneS3BucketRef(cr, bucketName)
	if _, err := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Create(ctx, cr, metav1.CreateOptions{}); err != nil {
		t.Fatalf("seed Duckling CR: %v", err)
	}
	if _, err := fakeK8s.Resource(s3BucketGVR).Create(ctx, bucketCR(bucketName, false, "InvalidBucketName"), metav1.CreateOptions{}); err != nil {
		t.Fatalf("seed Bucket CR: %v", err)
	}

	status, err := dc.GetProvisioningStatus(ctx, "org-bucket")
	if err != nil {
		t.Fatalf("GetProvisioningStatus: %v", err)
	}
	if status.DataStoreReadiness.Ready {
		t.Fatalf("expected DataStoreReadiness.Ready=false")
	}
	if status.DataStoreReadiness.Source != DataStoreReadinessSourceComposedBucket {
		t.Fatalf("source = %q, want %q", status.DataStoreReadiness.Source, DataStoreReadinessSourceComposedBucket)
	}
	if !strings.Contains(status.DataStoreReadiness.Message, "InvalidBucketName") {
		t.Fatalf("message = %q, want InvalidBucketName", status.DataStoreReadiness.Message)
	}
}

func TestDucklingEnsureDeletedReportsWhetherCRIsGone(t *testing.T) {
	dc, fakeK8s := newFakeDucklingClient()
	ctx := context.Background()

	if _, err := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Create(ctx, ducklingCR(ducklingName("org-delete"), nil), metav1.CreateOptions{}); err != nil {
		t.Fatalf("seed Duckling CR: %v", err)
	}

	gone, err := dc.EnsureDeleted(ctx, "org-delete")
	if err != nil {
		t.Fatalf("EnsureDeleted first call: %v", err)
	}
	if gone {
		t.Fatal("first EnsureDeleted call should request deletion and report gone=false")
	}

	gone, err = dc.EnsureDeleted(ctx, "org-delete")
	if err != nil {
		t.Fatalf("EnsureDeleted second call: %v", err)
	}
	if !gone {
		t.Fatal("second EnsureDeleted call should report gone=true after CR is absent")
	}
}

func TestBucketResourceRefNameReadsCrossplaneResourceRefs(t *testing.T) {
	for name, tc := range map[string]struct {
		object map[string]interface{}
		want   string
	}{
		"status.resourceRefs": {
			object: map[string]interface{}{"status": map[string]interface{}{
				"resourceRefs": []interface{}{map[string]interface{}{
					"apiVersion": s3BucketAPIVersion,
					"kind":       crossplaneBucketKind,
					"name":       "bucket-from-status-ref",
				}},
			}},
			want: "bucket-from-status-ref",
		},
		"spec.resourceRefs": {
			object: map[string]interface{}{"spec": map[string]interface{}{
				"resourceRefs": []interface{}{map[string]interface{}{
					"apiVersion": s3BucketAPIVersion,
					"kind":       crossplaneBucketKind,
					"name":       "bucket-from-spec-ref",
				}},
			}},
			want: "bucket-from-spec-ref",
		},
		"spec.crossplane.resourceRefs": {
			object: map[string]interface{}{"spec": map[string]interface{}{
				"crossplane": map[string]interface{}{
					"resourceRefs": []interface{}{map[string]interface{}{
						"apiVersion": s3BucketAPIVersion,
						"kind":       crossplaneBucketKind,
						"name":       "bucket-from-crossplane-ref",
					}},
				},
			}},
			want: "bucket-from-crossplane-ref",
		},
	} {
		t.Run(name, func(t *testing.T) {
			cr := &unstructured.Unstructured{Object: tc.object}
			if got := bucketResourceRefName(cr); got != tc.want {
				t.Fatalf("bucketResourceRefName = %q, want %q", got, tc.want)
			}
		})
	}
}
