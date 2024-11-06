package models

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
)

const ERROR_DETAIL_INVALID = "Invalid value"
const ERROR_DETAIL_MISSING = "Missing"

var NullReferenceError = fmt.Errorf("Unexpected null reference")

type ValidationErrorDetail struct {
	Location string
	Attr     string
	Detail   string
}

type ValidationError struct {
	Details []ValidationErrorDetail
}

func NewSingleValidationError(loc string, attr string, detail string) *ValidationError {
	return &ValidationError{
		Details: []ValidationErrorDetail{
			{
				Location: loc, Attr: attr, Detail: detail,
			},
		},
	}
}

func (e *ValidationError) Error() string {
	ser, _ := json.Marshal(e)
	return string(ser)
}

type APIError struct {
	Err        error
	StatusCode int
}

func (e *APIError) Error() string {
	return fmt.Sprintf("APIError with status code %d: %s", e.StatusCode, e.Err.Error())
}

func NewGenericClientError(msg string) *APIError {
	return &APIError{
		Err:        errors.New(msg),
		StatusCode: 400,
	}
}

func NewNotImplementedError() *APIError {
	return &APIError{
		Err:        errors.New("Not implemented"),
		StatusCode: 500,
	}
}

func NewGenericServerError(err error) *APIError {
	return &APIError{
		Err:        err,
		StatusCode: 500,
	}
}

func NewValidationError(err error) *APIError {
	return &APIError{
		Err:        err,
		StatusCode: 422,
	}
}

func NewNotFoundError(err error) *APIError {
	return &APIError{
		Err:        err,
		StatusCode: 404,
	}
}

func WriteError(w http.ResponseWriter, api_err *APIError) {
	if api_err != nil {
		enc := json.NewEncoder(w)
		w.WriteHeader(api_err.StatusCode)
		respBody := map[string]string{"detail": api_err.Err.Error()}
		if err := enc.Encode(&respBody); err != nil {
			slog.Warn(fmt.Sprint("Error writing error:", err.Error()))
		}
	}

	return
}
