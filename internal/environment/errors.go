package environment

// ErrorCode enumerates the distinguishable failure reasons. Every rejected
// operation surfaces one of these codes at every entry point (library, HTTP
// body and CLI), so failures can never be silent or ambiguous.
type ErrorCode string

const (
	// ErrCodeInvalidRequest: malformed input (empty target/name/holder ...).
	ErrCodeInvalidRequest ErrorCode = "InvalidRequest"
	// ErrCodeNameConflict: the requested name is already reserved by a
	// different, still active target.
	ErrCodeNameConflict ErrorCode = "NameConflict"
	// ErrCodeQuotaExhausted: the environment-count quota is full.
	ErrCodeQuotaExhausted ErrorCode = "QuotaExhausted"
	// ErrCodePortsExhausted: no host port can be allocated.
	ErrCodePortsExhausted ErrorCode = "PortsExhausted"
	// ErrCodeInUse: the operation is rejected because holders are present.
	ErrCodeInUse ErrorCode = "InUse"
	// ErrCodeNotReady: e.g. occupying an environment that never became ready.
	ErrCodeNotReady ErrorCode = "NotReady"
	// ErrCodeNotFound: no record exists for the given name.
	ErrCodeNotFound ErrorCode = "NotFound"
	// ErrCodeNotReclaimable: reclaim requires the Released state.
	ErrCodeNotReclaimable ErrorCode = "NotReclaimable"
	// ErrCodeProvisioner: the local process/container backend failed; the
	// in-memory record is rolled back so nothing is left half-modified.
	ErrCodeProvisioner ErrorCode = "ProvisionerFailure"
)

// Error is the structured, explainable error returned by all entry points.
type Error struct {
	Code      ErrorCode `json:"code"`
	Message   string    `json:"message"`
	Target    string    `json:"target"`
	RequestID string    `json:"request_id,omitempty"`
}

func (e *Error) Error() string {
	id := ""
	if e.RequestID != "" {
		id = " request=" + e.RequestID
	}
	tgt := ""
	if e.Target != "" {
		tgt = " target=" + e.Target
	}
	return string(e.Code) + tgt + id + ": " + e.Message
}

// NewError builds an *Error.
func NewError(code ErrorCode, message string) *Error {
	return &Error{Code: code, Message: message}
}

// WithContext enriches an error with target/request context while
// preserving its code.
func WithContext(err error, target, requestID string) *Error {
	if e, ok := err.(*Error); ok {
		ee := *e
		if ee.Target == "" {
			ee.Target = target
		}
		ee.RequestID = requestID
		return &ee
	}
	return &Error{Code: ErrCodeProvisioner, Message: errString(err), Target: target, RequestID: requestID}
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// AsError extracts the structured *Error from err, if present.
func AsError(err error) (*Error, bool) {
	for err != nil {
		if e, ok := err.(*Error); ok {
			return e, true
		}
		type unwrapper interface{ Unwrap() error }
		u, ok := err.(unwrapper)
		if !ok {
			return nil, false
		}
		err = u.Unwrap()
	}
	return nil, false
}

// CodeOf returns the structured code, or "" for non-controller errors.
func CodeOf(err error) ErrorCode {
	if e, ok := AsError(err); ok {
		return e.Code
	}
	return ""
}

func codeIs(err error, code ErrorCode) bool { return CodeOf(err) == code }

func IsInvalidRequest(err error) bool { return codeIs(err, ErrCodeInvalidRequest) }
func IsNotFound(err error) bool       { return codeIs(err, ErrCodeNotFound) }
func IsNameConflict(err error) bool   { return codeIs(err, ErrCodeNameConflict) }
func IsQuotaExhausted(err error) bool { return codeIs(err, ErrCodeQuotaExhausted) }
func IsPortsExhausted(err error) bool { return codeIs(err, ErrCodePortsExhausted) }
func IsInUse(err error) bool          { return codeIs(err, ErrCodeInUse) }
func IsNotReady(err error) bool       { return codeIs(err, ErrCodeNotReady) }
func IsNotReclaimable(err error) bool { return codeIs(err, ErrCodeNotReclaimable) }
func IsProvisioner(err error) bool    { return codeIs(err, ErrCodeProvisioner) }
