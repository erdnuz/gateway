package testutils

import "errors"

// Common test errors
var (
	ErrNotFound       = errors.New("not found")
	ErrUnauthorized   = errors.New("unauthorized")
	ErrTooManyRequest = errors.New("too many requests")
	ErrInternalServer = errors.New("internal server error")
)
