package common

// Electron statuses
const STATUS_NEW = "NEW_OBJECT"
const STATUS_STARTING = "STARTING"
const STATUS_RUNNING = "RUNNING"
const STATUS_DISPATCHING = "DISPATCHING"

// Terminal statuses
const STATUS_COMPLETED = "COMPLETED"
const STATUS_FAILED = "FAILED"

var validStatuses = map[string]bool{
	STATUS_NEW:         true,
	STATUS_STARTING:    true,
	STATUS_RUNNING:     true,
	STATUS_DISPATCHING: true,
	STATUS_COMPLETED:   true,
	STATUS_FAILED:      true,
}

func ValidateStatus(s string) bool {
	_, ok := validStatuses[s]
	return ok
}
