package cosmosapi

import "context"

type PatchOperation struct {
	// Op can be Add, Set, Replace, Remove, or Increment
	// See https://docs.microsoft.com/en-us/azure/cosmos-db/partial-document-update#supported-operations
	Op    string      `json:"op"`
	Path  string      `json:"path"`
	Value interface{} `json:"value,omitempty"`
}

type PatchDocumentOptions struct {
	PartitionKeyValue interface{}
	ConsistencyLevel  ConsistencyLevel
	SessionToken      string
	Condition         string
}

func (ops PatchDocumentOptions) AsHeaders() (map[string]string, error) {
	headers := map[string]string{}

	if ops.PartitionKeyValue != nil {
		v, err := MarshalPartitionKeyHeader(ops.PartitionKeyValue)
		if err != nil {
			return nil, err
		}
		headers[HEADER_PARTITIONKEY] = v
	}

	if ops.ConsistencyLevel != "" {
		headers[HEADER_CONSISTENCY_LEVEL] = string(ops.ConsistencyLevel)
	}

	if ops.SessionToken != "" {
		headers[HEADER_SESSION_TOKEN] = ops.SessionToken
	}

	return headers, nil
}

func (c *Client) PatchDocument(ctx context.Context, dbName, colName, id string, operations []PatchOperation, ops PatchDocumentOptions, out interface{}) (DocumentResponse, error) {
	headers, err := ops.AsHeaders()
	if err != nil {
		return DocumentResponse{}, err
	}

	link := createDocLink(dbName, colName, id)

	request := patchRequest{
		Condition:  ops.Condition,
		Operations: operations,
	}

	resp, err := c.patch(ctx, link, request, out, headers)
	if err != nil {
		return DocumentResponse{}, err
	}

	return parseDocumentResponse(resp), nil
}
