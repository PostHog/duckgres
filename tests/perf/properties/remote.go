package properties

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3Client interface {
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

// LoadS3 retrieves a completion manifest; inventory verification remains mandatory.
func LoadS3(ctx context.Context, client S3Client, uri string) (*Manifest, error) {
	u, err := s3URL(uri)
	if err != nil {
		return nil, err
	}
	out, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(u.Host), Key: aws.String(strings.TrimPrefix(u.Path, "/"))})
	if err != nil {
		return nil, fmt.Errorf("read completion manifest: %w", err)
	}
	defer func() { _ = out.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(out.Body, 16<<20))
	if err != nil {
		return nil, err
	}
	return Parse(data)
}

// VerifyInventory compares every live data-prefix object, including unexpected files.
func (m *Manifest) VerifyInventory(ctx context.Context, client S3Client) error {
	u, _ := s3URL(m.Config.DestinationPrefix)
	expected := make(map[string]File, len(m.Files))
	for _, f := range m.Files {
		expected[f.Key] = f
	}
	pager := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{Bucket: aws.String(u.Host), Prefix: aws.String(strings.TrimPrefix(u.Path, "/") + "data/")})
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("verify fixture inventory: %w", err)
		}
		for _, object := range page.Contents {
			key := aws.ToString(object.Key)
			f, ok := expected[key]
			if !ok || f.Size != aws.ToInt64(object.Size) || f.ETag != aws.ToString(object.ETag) {
				return fmt.Errorf("fixture inventory changed: unexpected or modified object")
			}
			delete(expected, key)
		}
	}
	if len(expected) > 0 {
		return fmt.Errorf("fixture inventory changed: %d missing objects", len(expected))
	}
	return nil
}

type GlueClient interface {
	GetTable(context.Context, *glue.GetTableInput, ...func(*glue.Options)) (*glue.GetTableOutput, error)
}

// VerifyAthenaTable checks the preprovisioned logical projection without mutating Glue.
func (m *Manifest) VerifyAthenaTable(ctx context.Context, client GlueClient, database, table string) error {
	out, err := client.GetTable(ctx, &glue.GetTableInput{DatabaseName: aws.String(database), Name: aws.String(table)})
	if err != nil {
		return fmt.Errorf("verify Athena table: %w", err)
	}
	if out.Table == nil || out.Table.StorageDescriptor == nil {
		return fmt.Errorf("athena table lacks storage descriptor")
	}
	sd := out.Table.StorageDescriptor
	if strings.EqualFold(out.Table.Parameters["projection.enabled"], "true") {
		return fmt.Errorf("athena partition projection must be disabled")
	}
	if sd.SerdeInfo == nil || aws.ToString(sd.SerdeInfo.SerializationLibrary) != "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe" {
		return fmt.Errorf("athena table requires standard Parquet SerDe")
	}

	if aws.ToString(sd.Location) != m.Config.DestinationPrefix+"data/" {
		return fmt.Errorf("athena table location differs from fixture")
	}
	if strings.EqualFold(out.Table.Parameters["parquet.column.index.access"], "true") || strings.EqualFold(sd.SerdeInfo.Parameters["parquet.column.index.access"], "true") {
		return fmt.Errorf("athena table must read Parquet columns by name")
	}
	required := map[string]string{"event": "string", "timestamp": "timestamp", "properties": "string", "properties_typed": "struct<$browser:string>"}
	if len(sd.Columns) != len(required) || len(out.Table.PartitionKeys) != 0 {
		return fmt.Errorf("athena table must expose exactly the supported unpartitioned columns")
	}
	for _, c := range sd.Columns {
		name := aws.ToString(c.Name)
		typ := strings.ReplaceAll(strings.ToLower(aws.ToString(c.Type)), "`", "")
		if required[name] != typ || typ == "" {
			return fmt.Errorf("athena table logical column mismatch")
		}
		delete(required, name)
	}
	if len(required) != 0 || aws.ToString(sd.InputFormat) != "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat" || aws.ToString(sd.OutputFormat) != "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat" {
		return fmt.Errorf("athena table must use the supported Parquet schema")
	}
	return nil
}
