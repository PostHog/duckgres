package properties

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	glueTypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type fakeS3 struct {
	pages []*s3.ListObjectsV2Output
	calls int
}

func (f *fakeS3) GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	panic("unused")
}
func (f *fakeS3) ListObjectsV2(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	out := f.pages[f.calls]
	f.calls++
	return out, nil
}
func TestVerifyInventory(t *testing.T) {
	raw := validManifest()
	f := raw["outputs"].([]File)[0]
	second := f
	second.Key = "derived/day/data/part-000001.parquet"
	raw["outputs"] = []File{f, second}
	data, _ := json.Marshal(raw)
	m, err := Parse(data)
	if err != nil {
		t.Fatal(err)
	}
	obj := func(f File) types.Object {
		return types.Object{Key: aws.String(f.Key), Size: aws.Int64(f.Size), ETag: aws.String(f.ETag)}
	}
	for _, mode := range []string{"valid", "extra", "size", "etag", "missing"} {
		t.Run(mode, func(t *testing.T) {
			a, b := obj(f), obj(second)
			if mode == "size" {
				b.Size = aws.Int64(1)
			}
			if mode == "etag" {
				b.ETag = aws.String("changed")
			}
			if mode == "extra" {
				b.Key = aws.String("derived/day/data/unexpected.parquet")
			}
			items := []types.Object{b}
			if mode == "missing" {
				items = nil
			}
			client := &fakeS3{pages: []*s3.ListObjectsV2Output{{Contents: []types.Object{a}, IsTruncated: aws.Bool(true), NextContinuationToken: aws.String("next")}, {Contents: items}}}
			err := m.VerifyInventory(context.Background(), client)
			if (err == nil) != (mode == "valid") {
				t.Fatalf("mode=%s err=%v", mode, err)
			}
			if client.calls != 2 {
				t.Fatal("pagination not consumed")
			}
		})
	}
}

type fakeGlue struct{ table *glueTypes.Table }

func (f fakeGlue) GetTable(context.Context, *glue.GetTableInput, ...func(*glue.Options)) (*glue.GetTableOutput, error) {
	return &glue.GetTableOutput{Table: f.table}, nil
}
func TestVerifyAthenaTable(t *testing.T) {
	data, _ := json.Marshal(validManifest())
	m, err := Parse(data)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"valid", "location", "variant", "format", "serde", "projection", "index_table", "index_serde", "custom_input", "custom_output"} {
		t.Run(mode, func(t *testing.T) {
			sd := &glueTypes.StorageDescriptor{SerdeInfo: &glueTypes.SerDeInfo{SerializationLibrary: aws.String("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")}, Location: aws.String(m.Config.DestinationPrefix + "data/"), InputFormat: aws.String("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"), OutputFormat: aws.String("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat")}
			for n, typ := range map[string]string{"event": "string", "timestamp": "timestamp", "properties": "string", "properties_typed": "struct<`$browser`:string>"} {
				sd.Columns = append(sd.Columns, glueTypes.Column{Name: aws.String(n), Type: aws.String(typ)})
			}
			if mode == "location" {
				sd.Location = aws.String("s3://example-fixture/other/")
			}
			if mode == "variant" {
				sd.Columns = append(sd.Columns, glueTypes.Column{Name: aws.String("properties_variant"), Type: aws.String("variant")})
			}
			if mode == "format" {
				sd.InputFormat = aws.String("text")
			}
			table := &glueTypes.Table{StorageDescriptor: sd}
			if mode == "serde" {
				sd.SerdeInfo.SerializationLibrary = aws.String("custom")
			}
			if mode == "projection" {
				table.Parameters = map[string]string{"projection.enabled": "true"}
			}
			if mode == "index_table" {
				table.Parameters = map[string]string{"parquet.column.index.access": "true"}
			}
			if mode == "index_serde" {
				sd.SerdeInfo.Parameters = map[string]string{"parquet.column.index.access": "true"}
			}
			if mode == "custom_input" {
				sd.InputFormat = aws.String("custom.parquet.InputFormat")
			}
			if mode == "custom_output" {
				sd.OutputFormat = aws.String("custom.parquet.OutputFormat")
			}
			err := m.VerifyAthenaTable(context.Background(), fakeGlue{table}, "example", "properties_events_supported")
			if (err == nil) != (mode == "valid") {
				t.Fatalf("mode %s err %v", mode, err)
			}
		})
	}
}
