package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/hashicorp/terraform-plugin-log/tflog"
	"github.com/spilliams/schema-terraform-provider/internal/slug"
	"github.com/spilliams/schema-terraform-provider/pkg/storage"
)

type Client struct {
	region    string
	tableName string
	keyARN    string

	ddb *dynamodb.Client
}

func NewClient(ctx context.Context, profile, region, tableName, keyARN string) (storage.RowStorer, error) {
	this := &Client{
		region:    region,
		tableName: tableName,
		keyARN:    keyARN,
	}

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(profile),
		config.WithRegion(region),
	)
	if err != nil {
		return nil, err
	}
	this.ddb = dynamodb.NewFromConfig(cfg)

	err = this.createTableIfNotExists(ctx)
	if err != nil {
		return nil, err
	}

	return this, nil
}

const (
	storageKeyType = "type"
	storageKeyID   = "id"

	storageAttrParentID = "parent_id"
	storageAttrLabel    = "label"
	storageAttrColumns  = "columns"

	storageGSIByParentAndLabel = "ByParentAndLabel"
	storageGSIByParent         = "ByParent"
	storageGSIByType           = "ByType"

	storageLSIByTypeAndLabel  = "ByTypeAndLabel"
	storageLSIByTypeAndParent = "ByTypeAndParent"
)

func (client *Client) createTableIfNotExists(ctx context.Context) error {
	describeTableOutput, err := client.ddb.DescribeTable(ctx,
		&dynamodb.DescribeTableInput{
			TableName: aws.String(client.tableName),
		},
	)
	if err == nil {
		// table already exists
		if describeTableOutput != nil {
			tflog.Debug(ctx, fmt.Sprintf("table %s exists", client.tableName), map[string]interface{}{"tableID": *describeTableOutput.Table.TableId})
		}
		return nil
	}

	var respErr *smithyhttp.ResponseError
	if ok := errors.As(err, &respErr); ok && respErr.Response != nil {
		statusCode := respErr.Response.StatusCode
		if statusCode != http.StatusBadRequest {
			tflog.Warn(ctx, fmt.Sprintf("DescribeTable failed with HTTP status %d: %s", statusCode, err.Error()))
		}
	} else {
		tflog.Warn(ctx, fmt.Sprintf("unexpected error during DescribeTable: %s", err.Error()))
		return err
	}

	input := &dynamodb.CreateTableInput{
		TableName: aws.String(client.tableName),
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String(storageKeyType),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String(storageKeyID),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String(storageAttrParentID),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String(storageAttrLabel),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String(storageKeyType),
				KeyType:       types.KeyTypeHash,
			},
			{
				AttributeName: aws.String(storageKeyID),
				KeyType:       types.KeyTypeRange,
			},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String(storageGSIByParentAndLabel),
				KeySchema: []types.KeySchemaElement{
					{
						AttributeName: aws.String(storageAttrParentID),
						KeyType:       types.KeyTypeHash,
					},
					{
						AttributeName: aws.String(storageAttrLabel),
						KeyType:       types.KeyTypeRange,
					},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
			{
				IndexName: aws.String(storageGSIByType),
				KeySchema: []types.KeySchemaElement{
					{
						AttributeName: aws.String(storageKeyType),
						KeyType:       types.KeyTypeHash,
					},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
		},
		LocalSecondaryIndexes: []types.LocalSecondaryIndex{
			{
				IndexName: aws.String(storageLSIByTypeAndLabel),
				KeySchema: []types.KeySchemaElement{
					{
						AttributeName: aws.String(storageKeyType),
						KeyType:       types.KeyTypeHash,
					},
					{
						AttributeName: aws.String(storageAttrLabel),
						KeyType:       types.KeyTypeRange,
					},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
			{
				IndexName: aws.String(storageLSIByTypeAndParent),
				KeySchema: []types.KeySchemaElement{
					{
						AttributeName: aws.String(storageKeyType),
						KeyType:       types.KeyTypeHash,
					},
					{
						AttributeName: aws.String(storageAttrParentID),
						KeyType:       types.KeyTypeRange,
					},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
		},
		BillingMode: types.BillingModePayPerRequest,
		SSESpecification: &types.SSESpecification{
			Enabled:        aws.Bool(true),
			SSEType:        types.SSETypeKms,
			KMSMasterKeyId: aws.String(client.keyARN),
		},
	}
	_, err = client.ddb.CreateTable(ctx, input)
	return err
}

var (
	ErrCannotDeleteRow      = errors.New("cannot delete row")
	ErrCollisionParentLabel = errors.New("a row with that parent and label already exists")
	ErrCollisionTypeLabel   = errors.New("a row with that type and label already exists")
	ErrNilQueryOutput       = errors.New("something went wrong: the query output was nil")
	ErrNotFoundRow          = errors.New("row not found")
	ErrTooManyFound         = errors.New("multiple exist where there must only be one")
)

func (client *Client) GetRowByID(ctx context.Context, rowType, id string) (storage.Row, error) {
	tflog.Debug(ctx, fmt.Sprintf("GetRowByID %q", id))
	output, err := client.ddb.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(client.tableName),
		Key: map[string]types.AttributeValue{
			storageKeyType: &types.AttributeValueMemberS{Value: rowType},
			storageKeyID:   &types.AttributeValueMemberS{Value: id},
		},
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return nil, err
	}
	if output.Item == nil {
		return nil, fmt.Errorf("%w: %q", ErrNotFoundRow, id)
	}
	return itemToRow(output.Item)
}

func (client *Client) GetRow(ctx context.Context, rowType, label string) (storage.Row, error) {
	tflog.Debug(ctx, fmt.Sprintf("GetRow %q %q", rowType, label))
	output, err := client.ddb.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(client.tableName),
		IndexName:              aws.String(storageLSIByTypeAndLabel),
		KeyConditionExpression: aws.String("#type = :type AND #label = :label"),
		ExpressionAttributeNames: map[string]string{
			"#type":  storageKeyType,
			"#label": storageAttrLabel,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":type":  &types.AttributeValueMemberS{Value: rowType},
			":label": &types.AttributeValueMemberS{Value: label},
		},
	})
	if err != nil {
		return nil, err
	}
	if output == nil || output.Items == nil {
		return nil, ErrNilQueryOutput
	}
	if len(output.Items) == 0 {
		return nil, fmt.Errorf("%w: type %q and label %q", ErrNotFoundRow, rowType, label)
	}
	if len(output.Items) > 1 {
		return nil, fmt.Errorf("%w: type %q and label %q", ErrTooManyFound, rowType, label)
	}

	return itemToRow(output.Items[0])
}

func (client *Client) CreateRow(ctx context.Context, rowType, label string) (storage.Row, error) {
	tflog.Debug(ctx, fmt.Sprintf("CreateRow %q %q", rowType, label))
	// make sure type+name doesn't collide
	output, err := client.ddb.Query(ctx, &dynamodb.QueryInput{
		TableName: aws.String(client.tableName),
		IndexName: aws.String(storageLSIByTypeAndLabel),

		KeyConditionExpression: aws.String("#type = :type AND #label = :label"),
		ExpressionAttributeNames: map[string]string{
			"#type":  storageKeyType,
			"#label": storageAttrLabel,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":type":  &types.AttributeValueMemberS{Value: rowType},
			":label": &types.AttributeValueMemberS{Value: label},
		},
	})
	if err != nil {
		return nil, err
	}
	if output == nil || output.Items == nil {
		return nil, ErrNilQueryOutput
	}
	if len(output.Items) > 0 {
		return nil, ErrCollisionTypeLabel
	}

	id := slug.Generate(rowType)

	// create item as long as type+ID doesn't collide
	_, err = client.ddb.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(client.tableName),
		Item: map[string]types.AttributeValue{
			storageKeyType:   &types.AttributeValueMemberS{Value: rowType},
			storageKeyID:     &types.AttributeValueMemberS{Value: id},
			storageAttrLabel: &types.AttributeValueMemberS{Value: label},
		},
		ExpressionAttributeNames: map[string]string{
			"#type": storageKeyType,
			"#id":   storageKeyID,
		},
		ConditionExpression: aws.String("attribute_not_exists(#type) AND attribute_not_exists(#id)"),
	})
	if err != nil {
		return nil, err
	}

	return &row{
		RowType:  rowType,
		RowID:    id,
		RowLabel: label,
	}, nil
}

func (client *Client) CreateChild(ctx context.Context, rowType, label, parentType, parentID string, columns map[string]interface{}) (storage.Row, error) {
	tflog.Debug(ctx, fmt.Sprintf("CreateChild %q %q %q %q", rowType, label, parentType, parentID))
	id := slug.Generate(rowType)
	object := &row{
		RowType:    rowType,
		RowID:      id,
		RowLabel:   label,
		RowColumns: columns,
	}

	// make sure parent exists
	parent, err := client.GetRowByID(ctx, parentType, parentID)
	if err != nil {
		return nil, err
	}

	object.RowParentID = parent.ID()

	// make sure label is unique within the parent
	output, err := client.ddb.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(client.tableName),
		IndexName:              aws.String(storageGSIByParentAndLabel),
		KeyConditionExpression: aws.String("#parent_id = :parent_id AND #label = :label"),
		ExpressionAttributeNames: map[string]string{
			"#parent_id": storageAttrParentID,
			"#label":     storageAttrLabel,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":parent_id": &types.AttributeValueMemberS{Value: parentID},
			":label":     &types.AttributeValueMemberS{Value: label},
		},
	})
	if err != nil {
		return nil, err
	}
	if output == nil || output.Items == nil {
		return nil, ErrNilQueryOutput
	}
	if len(output.Items) > 0 {
		return nil, ErrCollisionParentLabel
	}

	_, err = client.ddb.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(client.tableName),
		Item: map[string]types.AttributeValue{
			storageKeyType:      &types.AttributeValueMemberS{Value: rowType},
			storageKeyID:        &types.AttributeValueMemberS{Value: id},
			storageAttrLabel:    &types.AttributeValueMemberS{Value: label},
			storageAttrParentID: &types.AttributeValueMemberS{Value: parentID},
			storageAttrColumns:  &types.AttributeValueMemberM{Value: columnsToMap(columns)},
		},
		ExpressionAttributeNames: map[string]string{
			"#type": storageKeyType,
			"#id":   storageKeyID,
		},
		ConditionExpression: aws.String("attribute_not_exists(#type) AND attribute_not_exists(#id)"),
	})
	if err != nil {
		return nil, err
	}

	return object, nil
}

func (client *Client) GetChild(ctx context.Context, label, parentID string) (storage.Row, error) {
	tflog.Debug(ctx, fmt.Sprintf("GetChild %q %q", label, parentID))
	output, err := client.ddb.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(client.tableName),
		IndexName:              aws.String(storageGSIByParentAndLabel),
		KeyConditionExpression: aws.String("#parent_id = :parent_id AND #label = :label"),
		ExpressionAttributeNames: map[string]string{
			"#parent_id": storageAttrParentID,
			"#label":     storageAttrLabel,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":parent_id": &types.AttributeValueMemberS{Value: parentID},
			":label":     &types.AttributeValueMemberS{Value: label},
		},
	})
	if err != nil {
		return nil, err
	}
	if output == nil || output.Items == nil {
		return nil, ErrNilQueryOutput
	}
	if len(output.Items) == 0 {
		return nil, fmt.Errorf("%w with parent ID %q and label %q", ErrNotFoundRow, parentID, label)
	}
	if len(output.Items) > 1 {
		return nil, fmt.Errorf("%w: parent ID %q and label %q", ErrTooManyFound, parentID, label)
	}

	return itemToRow(output.Items[0])
}

func (client *Client) ListRows(ctx context.Context, rowType, labelFilter, parentIDFilter string) ([]storage.Row, error) {
	tflog.Debug(ctx, fmt.Sprintf("ListRows %q %q %q", rowType, labelFilter, parentIDFilter))
	input := &dynamodb.QueryInput{
		TableName:              aws.String(client.tableName),
		IndexName:              aws.String(storageGSIByType),
		KeyConditionExpression: aws.String("#type = :type"),
		ExpressionAttributeNames: map[string]string{
			"#type": storageKeyType,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":type": &types.AttributeValueMemberS{Value: rowType},
		},
	}

	filterExprs := []string{}
	if labelFilter != "" {
		filterExprs = append(filterExprs, "contains(#label, :label)")
		input.ExpressionAttributeNames["#label"] = storageAttrLabel
		input.ExpressionAttributeValues[":label"] = &types.AttributeValueMemberS{Value: labelFilter}
	}
	if parentIDFilter != "" {
		filterExprs = append(filterExprs, "#parent_id = :parent_id")
		input.ExpressionAttributeNames["#parent_id"] = storageAttrParentID
		input.ExpressionAttributeValues[":parent_id"] = &types.AttributeValueMemberS{Value: parentIDFilter}
	}
	if len(filterExprs) > 0 {
		input.FilterExpression = aws.String(strings.Join(filterExprs, " AND "))
	}

	output, err := client.ddb.Query(ctx, input)
	if err != nil {
		return nil, err
	}
	if output == nil || output.Items == nil {
		return nil, ErrNilQueryOutput
	}
	rows := make([]storage.Row, len(output.Items))
	for i, item := range output.Items {
		rows[i], err = itemToRow(item)
		if err != nil {
			return nil, err
		}
	}
	return rows, nil
}

func (client *Client) UpdateRow(ctx context.Context, rowType, id, newLabel string) (storage.Row, error) {
	tflog.Debug(ctx, fmt.Sprintf("UpdatRow %q %q %q", rowType, id, newLabel))
	// ensure new label is available
	this, err := client.GetRowByID(ctx, rowType, id)
	if err != nil {
		return nil, err
	}
	_, err = client.GetChild(ctx, newLabel, this.ParentID())
	if err == nil {
		return nil, ErrCollisionParentLabel
	}
	if !errors.Is(err, ErrNotFoundRow) {
		return nil, err
	}

	output, err := client.ddb.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(client.tableName),
		Key: map[string]types.AttributeValue{
			storageKeyType: &types.AttributeValueMemberS{Value: rowType},
			storageKeyID:   &types.AttributeValueMemberS{Value: id},
		},
		UpdateExpression: aws.String("SET #label = :new_label"),
		ExpressionAttributeNames: map[string]string{
			"#label": storageAttrLabel,
			"#type":  storageKeyType,
			"#id":    storageKeyID,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":new_label": &types.AttributeValueMemberS{Value: newLabel},
		},
		ConditionExpression: aws.String("attribute_not_exists(#type) AND attribute_not_exists(#id)"),
		ReturnValues:        types.ReturnValueAllNew,
	})
	if err != nil {
		return nil, err
	}
	if output == nil || output.Attributes == nil {
		return nil, ErrNilQueryOutput
	}
	return itemToRow(output.Attributes)
}

func (client *Client) UpdateChild(ctx context.Context, childType, childID, newChildLabel, parentType, newParentID string) (storage.Row, error) {
	tflog.Debug(ctx, fmt.Sprintf("UpdateChild %q %q %q %q %q", childType, childID, newChildLabel, parentType, newParentID))
	// ensure new parent exists
	_, err := client.GetRowByID(ctx, parentType, newParentID)
	if err != nil {
		return nil, err
	}

	// ensure new label is available
	_, err = client.GetChild(ctx, newChildLabel, newParentID)
	if err == nil {
		return nil, ErrCollisionParentLabel
	}
	if !errors.Is(err, ErrNotFoundRow) {
		return nil, err
	}

	// update the item
	output, err := client.ddb.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(client.tableName),
		Key: map[string]types.AttributeValue{
			storageKeyType: &types.AttributeValueMemberS{Value: childType},
			storageKeyID:   &types.AttributeValueMemberS{Value: childID},
		},
		UpdateExpression: aws.String("SET #label = :new_label, #parent_id = :new_parent_id"),
		ExpressionAttributeNames: map[string]string{
			"#label":     storageAttrLabel,
			"#parent_id": storageAttrParentID,
			"#type":      storageKeyType,
			"#id":        storageKeyID,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":new_label":     &types.AttributeValueMemberS{Value: newChildLabel},
			":new_parent_id": &types.AttributeValueMemberS{Value: newParentID},
		},
		ConditionExpression: aws.String("attribute_not_exists(#type) AND attribute_not_exists(#id)"),
		ReturnValues:        types.ReturnValueAllNew,
	})
	if err != nil {
		return nil, err
	}
	if output == nil || output.Attributes == nil {
		return nil, ErrNilQueryOutput
	}
	return itemToRow(output.Attributes)
}

func (client *Client) UpdateColumn(ctx context.Context, rowType, rowID, columnName string, columnValue interface{}) error {
	tflog.Debug(ctx, fmt.Sprintf("UpdateColumn %q %q %q %q", rowType, rowID, columnName, columnValue))

	value := ifaceToAttributeValue(columnValue)

	_, err := client.ddb.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(client.tableName),
		Key: map[string]types.AttributeValue{
			storageKeyType: &types.AttributeValueMemberS{Value: rowType},
			storageKeyID:   &types.AttributeValueMemberS{Value: rowID},
		},
		UpdateExpression: aws.String("SET #columns.#key = :value"),
		ExpressionAttributeNames: map[string]string{
			"#columns": storageAttrColumns,
			"#key":     columnName,
			"#type":    storageKeyType,
			"#id":      storageKeyID,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":value": value,
		},
		ConditionExpression: aws.String("attribute_exists(#type) AND attribute_exists(#id)"),
	})
	return err
}

func (client *Client) UpdateColumns(ctx context.Context, rowType, rowID string, columns map[string]interface{}) error {
	tflog.Debug(ctx, fmt.Sprintf("UpdateColumns %q %q", rowType, rowID))
	_, err := client.ddb.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(client.tableName),
		Key: map[string]types.AttributeValue{
			storageKeyType: &types.AttributeValueMemberS{Value: rowType},
			storageKeyID:   &types.AttributeValueMemberS{Value: rowID},
		},
		UpdateExpression: aws.String("SET #columns = :new_columns"),
		ExpressionAttributeNames: map[string]string{
			"#columns": storageAttrColumns,
			"#type":    storageKeyType,
			"#id":      storageKeyID,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":new_columns": &types.AttributeValueMemberM{Value: columnsToMap(columns)},
		},
		ConditionExpression: aws.String("attribute_exists(#type) AND attribute_exists(#id)"),
	})
	return err
}

func (client *Client) DeleteRow(ctx context.Context, rowType, childType, id string) error {
	tflog.Debug(ctx, fmt.Sprintf("DeleteRow %q %q %q", rowType, childType, id))
	// ensure this row does not have any children
	if len(childType) > 0 {
		output, err := client.ddb.Query(ctx, &dynamodb.QueryInput{
			TableName:              aws.String(client.tableName),
			IndexName:              aws.String(storageLSIByTypeAndParent),
			KeyConditionExpression: aws.String("#type = :type AND #parent_id = :parent_id"),
			ExpressionAttributeNames: map[string]string{
				"#type":      storageKeyType,
				"#parent_id": storageAttrParentID,
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":type":      &types.AttributeValueMemberS{Value: childType},
				":parent_id": &types.AttributeValueMemberS{Value: id},
			},
		})
		if err != nil {
			return err
		}
		if output == nil || output.Items == nil {
			return ErrNilQueryOutput
		}
		if len(output.Items) > 0 {
			return fmt.Errorf("%s %s has children: %w", rowType, id, ErrCannotDeleteRow)
		}
	}

	_, err := client.ddb.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(client.tableName),
		Key: map[string]types.AttributeValue{
			storageKeyType: &types.AttributeValueMemberS{Value: rowType},
			storageKeyID:   &types.AttributeValueMemberS{Value: id},
		},
		ExpressionAttributeNames: map[string]string{
			"#type": storageKeyType,
			"#id":   storageKeyID,
		},
		ConditionExpression: aws.String("attribute_exists(#type) and attribute_exists(#id)"),
	})
	return err
}
