package models

import "encoding/json"

type APIIntrospectionResponse struct {
	Routes []string
}

func (r *APIIntrospectionResponse) EncodeJSON(enc *json.Encoder) *APIError {
	json_err := enc.Encode(r)
	if json_err != nil {
		return NewGenericServerError(json_err)
	}
	return nil
}
