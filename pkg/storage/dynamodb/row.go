package dynamodb

import (
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type row struct {
	RowType     string                 `dynamodbav:"type"`
	RowID       string                 `dynamodbav:"id"`
	RowLabel    string                 `dynamodbav:"label"`
	RowParentID string                 `dynamodbav:"parent_id"`
	RowColumns  map[string]interface{} `dynamodbav:"columns"`
}

func itemToRow(item map[string]types.AttributeValue) (*row, error) {
	var r row
	err := attributevalue.UnmarshalMap(item, &r)
	if err != nil {
		return nil, err
	}
	return &r, nil
}

func ifaceToAttributeValue(in interface{}) types.AttributeValue {
	var out types.AttributeValue
	if vString, isString := in.(string); isString {
		out = &types.AttributeValueMemberS{Value: vString}
	}
	if vStringList, isStringList := in.([]string); isStringList {
		out = &types.AttributeValueMemberSS{Value: vStringList}
	}
	return out
}

func columnsToMap(columns map[string]interface{}) map[string]types.AttributeValue {
	awsmap := make(map[string]types.AttributeValue)
	for k, v := range columns {
		awsmap[k] = ifaceToAttributeValue(v)
	}
	return awsmap
}

func (r *row) Type() string                    { return r.RowType }
func (r *row) ID() string                      { return r.RowID }
func (r *row) Label() string                   { return r.RowLabel }
func (r *row) ParentID() string                { return r.RowParentID }
func (r *row) Columns() map[string]interface{} { return r.RowColumns }
