package models

import (
	"encoding/json"
	"time"
)

type ElectronMeta struct {
	TaskGroupId      int            `json:"task_group_id"`
	Name             string         `json:"name"`
	SubdispatchId    string         `json:"sub_dispatch_id,omitempty"`
	Executor         string         `json:"executor"`
	ExecutorData     string         `json:"-"`
	ExecutorDataJSON map[string]any `json:"executor_data"`
	Status           string         `json:"status"`
	StartTime        *time.Time     `json:"start_time"`
	EndTime          *time.Time     `json:"end_time"`

	// Deprecated; always return false and don't save in db
	QElectronDataExists bool `json:"qelectron_data_exists"`
}

type ElectronAssets struct {
	Function       AssetDetails `json:"function"`
	FunctionString AssetDetails `json:"function_string"`
	Value          AssetDetails `json:"value"`
	Output         AssetDetails `json:"output"`
	Error          AssetDetails `json:"error"`
	Stdout         AssetDetails `json:"stdout"`
	Stderr         AssetDetails `json:"stderr"`
	Hooks          AssetDetails `json:"hooks"`
}

type ElectronSchema struct {
	NodeId   int            `json:"id"`
	Metadata ElectronMeta   `json:"metadata"`
	Assets   ElectronAssets `json:"assets"`
}

func (a *ElectronAssets) AttrsByName() map[string]*AssetDetails {
	return map[string]*AssetDetails{
		"function":        &a.Function,
		"function_string": &a.FunctionString,
		"value":           &a.Value,
		"output":          &a.Output,
		"error":           &a.Error,
		"stdout":          &a.Stdout,
		"stderr":          &a.Stderr,
		"hooks":           &a.Hooks,
	}
}

func (e *ElectronMeta) validateRequest() *APIError {

	// TODO: validate attributes:
	// Status
	// StartTime
	// EndTime

	return nil
}

func (e *ElectronAssets) validateRequest() *APIError {

	// TODO: validate
	return nil
}

func (e *ElectronSchema) validateRequest() *APIError {
	serialized, err := json.Marshal(e.Metadata.ExecutorDataJSON)
	if err != nil {
		return NewValidationError(err)
	}

	// TODO: validate all fields:
	e.Metadata.ExecutorData = string(serialized)
	validation_err := e.Metadata.validateRequest()
	if validation_err != nil {
		return validation_err
	}
	validation_err = e.Assets.validateRequest()
	if validation_err != nil {
		return validation_err
	}

	return nil
}

func (e *ElectronSchema) validateResponse() *APIError {
	err := json.Unmarshal([]byte(e.Metadata.ExecutorData), &e.Metadata.ExecutorDataJSON)
	if err != nil {
		return NewGenericServerError(err)
	}

	// Deprecated
	e.Metadata.QElectronDataExists = false

	return nil
}

type ElectronStatusUpdate struct {
	Status    string     `json:"status"`
	StartTime *time.Time `json:"start_time"`
	EndTime   *time.Time `json:"end_time"`
}

func (u *ElectronStatusUpdate) DecodeJSON(dec *json.Decoder) *APIError {
	dec_err := dec.Decode(u)
	if dec_err != nil {
		wrapped := NewValidationError(dec_err)
		return wrapped
	}
	return nil
}
