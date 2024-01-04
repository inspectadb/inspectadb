package errs

import "errors"

var (
	FailedToLoad                = errors.New("failed to load config")
	InvalidDSN                  = errors.New("invalid dsn in config")
	InvalidPort                 = errors.New("invalid port in dsn")
	DuplicateDriverRegistration = errors.New("duplicate driver registration")
	UnknownDriverRequested      = errors.New("unknown driver requested")
	FailedToVerifyLicense       = errors.New("failed to verify license, exiting")
	ProfileAlreadyStarted       = errors.New("attempting to start an already started profile")
	ProfileAlreadyEnded         = errors.New("attempting to end an unstarted profile")
	FailedToOpenDB              = errors.New("failed to open db")
	FailedToConnectToDB         = errors.New("failed to connect to db")
	FailedToBeginTransaction    = errors.New("failed to begin transaction")
	FailedToExecuteTransaction  = errors.New("failed to execute transaction")
	FailedToCommitTransaction   = errors.New("failed to commit transaction")
	FailedToCreateHistoryTable  = errors.New("failed to create history table")
	FailedToReadStubFile        = errors.New("failed to read stub file")
	FailedToSendTelemetry       = errors.New("failed to send telemetry request. this will not stop execution")
	FailedToGetServerVersion    = errors.New("failed to get server version")
	FailedToGetTriggerTables    = errors.New("failed to get trigger tables")
)
