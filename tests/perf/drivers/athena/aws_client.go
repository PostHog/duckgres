package athena

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awsathena "github.com/aws/aws-sdk-go-v2/service/athena"
	athenatypes "github.com/aws/aws-sdk-go-v2/service/athena/types"
)

type athenaAPI interface {
	StartQueryExecution(context.Context, *awsathena.StartQueryExecutionInput, ...func(*awsathena.Options)) (*awsathena.StartQueryExecutionOutput, error)
	GetQueryExecution(context.Context, *awsathena.GetQueryExecutionInput, ...func(*awsathena.Options)) (*awsathena.GetQueryExecutionOutput, error)
	GetQueryResults(context.Context, *awsathena.GetQueryResultsInput, ...func(*awsathena.Options)) (*awsathena.GetQueryResultsOutput, error)
	StopQueryExecution(context.Context, *awsathena.StopQueryExecutionInput, ...func(*awsathena.Options)) (*awsathena.StopQueryExecutionOutput, error)
}

type awsClient struct {
	api athenaAPI
}

func newAWSClient(ctx context.Context, region string) (*awsClient, error) {
	options := []func(*awsconfig.LoadOptions) error{}
	if region != "" {
		options = append(options, awsconfig.WithRegion(region))
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, options...)
	if err != nil {
		return nil, fmt.Errorf("load AWS configuration for Athena: %w", err)
	}
	return &awsClient{api: awsathena.NewFromConfig(cfg)}, nil
}

func (c *awsClient) StartQuery(ctx context.Context, input StartQueryInput) (string, error) {
	output, err := c.api.StartQueryExecution(ctx, &awsathena.StartQueryExecutionInput{
		QueryString: aws.String(input.SQL),
		WorkGroup:   aws.String(input.WorkGroup),
		QueryExecutionContext: &athenatypes.QueryExecutionContext{
			Catalog:  aws.String(input.Catalog),
			Database: aws.String(input.Database),
		},
		ResultConfiguration: &athenatypes.ResultConfiguration{
			OutputLocation: aws.String(input.OutputLocation),
		},
		ResultReuseConfiguration: &athenatypes.ResultReuseConfiguration{
			ResultReuseByAgeConfiguration: &athenatypes.ResultReuseByAgeConfiguration{
				Enabled: input.ResultReuseEnabled,
			},
		},
	})
	if err != nil {
		return "", err
	}
	if output.QueryExecutionId == nil || *output.QueryExecutionId == "" {
		return "", fmt.Errorf("athena returned an empty query execution ID")
	}
	return *output.QueryExecutionId, nil
}

func (c *awsClient) GetQuery(ctx context.Context, queryID string) (QueryExecution, error) {
	output, err := c.api.GetQueryExecution(ctx, &awsathena.GetQueryExecutionInput{QueryExecutionId: aws.String(queryID)})
	if err != nil {
		return QueryExecution{}, err
	}
	if output.QueryExecution == nil || output.QueryExecution.Status == nil {
		return QueryExecution{}, fmt.Errorf("athena returned incomplete execution state")
	}
	execution := QueryExecution{
		State:             QueryState(output.QueryExecution.Status.State),
		StateChangeReason: aws.ToString(output.QueryExecution.Status.StateChangeReason),
	}
	if output.QueryExecution.ResultConfiguration != nil {
		execution.OutputLocation = aws.ToString(output.QueryExecution.ResultConfiguration.OutputLocation)
	}
	if output.QueryExecution.EngineVersion != nil {
		execution.EngineVersion = aws.ToString(output.QueryExecution.EngineVersion.EffectiveEngineVersion)
	}
	if statistics := output.QueryExecution.Statistics; statistics != nil {
		execution.Statistics = QueryStatistics{
			QueueDuration:    millis(statistics.QueryQueueTimeInMillis),
			PlanningDuration: millis(statistics.QueryPlanningTimeInMillis),
			EngineDuration:   millis(statistics.EngineExecutionTimeInMillis),
			ServiceDuration:  millis(statistics.TotalExecutionTimeInMillis),
			BytesScanned:     aws.ToInt64(statistics.DataScannedInBytes),
			DPUCount:         aws.ToFloat64(statistics.DpuCount),
		}
		if statistics.ResultReuseInformation != nil {
			execution.Statistics.ResultReused = statistics.ResultReuseInformation.ReusedPreviousResult
		}
	}
	return execution, nil
}

func (c *awsClient) GetResults(ctx context.Context, queryID, nextToken string) (ResultPage, error) {
	input := &awsathena.GetQueryResultsInput{
		QueryExecutionId: aws.String(queryID),
		MaxResults:       aws.Int32(1000),
	}
	if nextToken != "" {
		input.NextToken = aws.String(nextToken)
	}
	output, err := c.api.GetQueryResults(ctx, input)
	if err != nil {
		return ResultPage{}, err
	}
	page := ResultPage{NextToken: aws.ToString(output.NextToken)}
	if output.ResultSet != nil {
		page.RowCount = int64(len(output.ResultSet.Rows))
	}
	return page, nil
}

func (c *awsClient) StopQuery(ctx context.Context, queryID string) error {
	_, err := c.api.StopQueryExecution(ctx, &awsathena.StopQueryExecutionInput{QueryExecutionId: aws.String(queryID)})
	return err
}

func millis(value *int64) time.Duration {
	return time.Duration(aws.ToInt64(value)) * time.Millisecond
}
