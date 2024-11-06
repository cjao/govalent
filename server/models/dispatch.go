package models

import (
	"encoding/json"
	"time"

	"github.com/casey/govalent/server/common"
	"github.com/google/uuid"
)

type LatticeMeta struct {
	Name                     string         `json:"name"`
	Executor                 string         `json:"executor"`
	ExecutorData             string         `json:"-"`
	ExecutorDataJSON         map[string]any `json:"executor_data"`
	WorkflowExecutor         string         `json:"workflow_executor"`
	WorkflowExecutorData     string         `json:"-"`
	WorkflowExecutorDataJSON map[string]any `json:"workflow_executor_data"`

	PythonVersion   string `json:"python_version"`
	CovalentVersion string `json:"covalent_version"`
}

type LatticeAssets struct {
	WorkflowFunction       AssetDetails `json:"workflow_function"`
	WorkflowFunctionString AssetDetails `json:"workflow_function_string"`
	Doc                    AssetDetails `json:"doc"`
	Inputs                 AssetDetails `json:"inputs"`
	Hooks                  AssetDetails `json:"hooks"`
}

func (a *LatticeAssets) AttrsByName() map[string]*AssetDetails {
	return map[string]*AssetDetails{
		"workflow_function":        &a.WorkflowFunction,
		"workflow_function_string": &a.WorkflowFunctionString,
		"doc":                      &a.Doc,
		"inputs":                   &a.Inputs,
		"hooks":                    &a.Hooks,
	}
}

type LatticeSchema struct {
	Metadata       LatticeMeta   `json:"metadata"`
	Assets         LatticeAssets `json:"assets"`
	TransportGraph Graph         `json:"transport_graph"`
}

type DispatchMeta struct {
	DispatchId     string     `json:"dispatch_id"`
	RootDispatchId string     `json:"root_dispatch_id"`
	Status         string     `json:"status"`
	StartTime      *time.Time `json:"start_time"`
	EndTime        *time.Time `json:"end_time"`

	CreatedAt time.Time
	UpdatedAt time.Time
}

type DispatchAssets struct {
	Result AssetDetails `json:"result"`
	Error  AssetDetails `json:"error"`
}

func (a *DispatchAssets) AttrsByName() map[string]*AssetDetails {
	return map[string]*AssetDetails{
		"result": &a.Result,
		"error":  &a.Error,
	}
}

type DispatchSchema struct {
	Metadata DispatchMeta   `json:"metadata"`
	Assets   DispatchAssets `json:"assets"`
	Lattice  LatticeSchema  `json:"lattice"`
}

func (l *LatticeMeta) validateRequest() *APIError {
	// Encode ExecutorDataJSON as a string
	serialized, err := json.Marshal(l.ExecutorDataJSON)
	if err != nil {
		return NewValidationError(err)
	}
	l.ExecutorData = string(serialized)

	serialized, err = json.Marshal(l.WorkflowExecutorDataJSON)
	if err != nil {
		return NewValidationError(err)
	}
	l.WorkflowExecutorData = string(serialized)

	return nil
}
func (l *LatticeMeta) validateResponse() *APIError {
	err := json.Unmarshal([]byte(l.ExecutorData), &l.ExecutorDataJSON)
	if err != nil {
		return NewGenericServerError(err)
	}
	err = json.Unmarshal([]byte(l.WorkflowExecutorData), &l.WorkflowExecutorDataJSON)
	if err != nil {
		return NewGenericServerError(err)
	}
	return nil
}

func (l *LatticeAssets) validateRequest() *APIError {
	// TODO: validate
	return nil
}

func (l *LatticeSchema) validateRequest() *APIError {
	err := (&l.Metadata).validateRequest()
	if err != nil {
		return err
	}
	err = (&l.Assets).validateRequest()
	if err != nil {
		return err
	}
	err = (&l.TransportGraph).validateRequest()
	if err != nil {
		return err
	}
	return nil
}

func (l *LatticeSchema) validateResponse() *APIError {
	err := (&l.Metadata).validateResponse()
	if err != nil {
		return err
	}
	err = (&l.TransportGraph).validateResponse()
	if err != nil {
		return err
	}
	return nil
}

func (d *DispatchMeta) validateRequest() *APIError {
	if len(d.DispatchId) == 0 {
		d.DispatchId = uuid.NewString()
	}
	d.CreatedAt = time.Now().UTC()
	d.UpdatedAt = d.CreatedAt

	if !common.ValidateStatus(d.Status) {
		detail := NewSingleValidationError("body", "status", ERROR_DETAIL_INVALID)
		return NewValidationError(detail)
	}

	return nil
}

func (d *DispatchSchema) validateRequest() *APIError {

	err := (&d.Metadata).validateRequest()
	if err != nil {
		return err
	}
	return (&d.Lattice).validateRequest()
}

func (d *DispatchSchema) validateResponse() *APIError {
	return (&d.Lattice).validateResponse()
}

func (m *DispatchSchema) EncodeJSON(enc *json.Encoder) *APIError {
	err := m.validateResponse()
	if err != nil {
		return NewGenericServerError(err)
	}
	json_err := enc.Encode(m)
	if json_err != nil {
		return NewGenericServerError(json_err)
	}
	return nil
}

func (m *DispatchSchema) DecodeJSON(dec *json.Decoder) *APIError {
	dec_err := dec.Decode(m)

	if dec_err != nil {
		wrapped := NewValidationError(dec_err)
		return wrapped
	}
	err := m.validateRequest()
	if err != nil {
		wrapped := NewValidationError(err)
		return wrapped
	}
	return nil
}

type GetBulkDispatchesResponse struct {
	Records []DispatchMeta `json:"records"`
}

func (r *GetBulkDispatchesResponse) EncodeJSON(enc *json.Encoder) *APIError {
	json_err := enc.Encode(r)
	if json_err != nil {
		return NewGenericServerError(json_err)
	}
	return nil
}
