package models

import (
	"encoding/json"

	"github.com/casey/govalent/server/common"
)

type ConfigResponse struct {
	*common.Config
}

func (c *ConfigResponse) EncodeJSON(enc *json.Encoder) *APIError {
	enc_err := enc.Encode(c)
	if enc_err != nil {
		return NewGenericServerError(enc_err)
	}
	return nil
}

type EdgeMetadata struct {
	Name      string `json:"edge_name"`
	ParamType string `json:"param_type"`
	ArgIndex  *int   `json:"arg_index"`
}

type Edge struct {
	Source   int          `json:"source"`
	Target   int          `json:"target"`
	Metadata EdgeMetadata `json:"metadata"`
}

type Graph struct {
	Nodes []ElectronSchema `json:"nodes"`
	Links []Edge           `json:"links"`
}

func (g *Graph) validateRequest() *APIError {
	for i := range g.Nodes {
		err := (&g.Nodes[i]).validateRequest()
		if err != nil {
			return err
		}
	}
	return nil
}

func (g *Graph) validateResponse() *APIError {
	for i := range g.Nodes {
		err := (&g.Nodes[i]).validateResponse()
		if err != nil {
			return err
		}
	}
	return nil
}
