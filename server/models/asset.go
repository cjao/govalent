package models

import (
	"encoding/json"
	"errors"
)

type AssetDetails struct {
	DigestAlg string `json:"digest_alg"`
	Digest    string `json:"digest"`
	Uri       string `json:"uri"`
	RemoteUri string `json:"remote_uri"`
	Size      int    `json:"size"`
}

type AssetPublicSchema struct {
	AssetDetails
	Key string `json:"key"`
}

type BulkAssetPostBody struct {
	Assets []AssetPublicSchema `json:"assets"`
}

func (p *BulkAssetPostBody) validateRequest() *APIError {
	for _, item := range p.Assets {
		if len(item.Key) == 0 {
			return NewValidationError(errors.New("Key must not be empty"))
		}
	}
	return nil
}

func (p *BulkAssetPostBody) DecodeJSON(dec *json.Decoder) *APIError {
	dec_err := dec.Decode(p)
	if dec_err != nil {
		return NewValidationError(dec_err)
	}
	validation_err := p.validateRequest()
	if validation_err != nil {
		return validation_err
	}
	return nil
}

type ClientURI struct {
	Uri string `json:"remote_uri"` // Remote relative to the client
}

type BulkAssetPostResponse struct {
	Assets []AssetPublicSchema `json:"assets"`
}

func (p *BulkAssetPostResponse) EncodeJSON(enc *json.Encoder) *APIError {
	json_err := enc.Encode(p)
	if json_err != nil {
		return NewGenericServerError(json_err)
	}
	return nil
}

type BulkAssetGetResponse struct {
	Assets []AssetPublicSchema `json:"assets"`
}

func (r *BulkAssetGetResponse) EncodeJSON(enc *json.Encoder) *APIError {
	json_err := enc.Encode(r)
	if json_err != nil {
		return NewGenericServerError(json_err)
	}
	return nil
}
