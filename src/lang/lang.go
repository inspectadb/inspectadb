package lang

const (
	DriftDetected   = "drift detected between %s (trigger table) and %s (change table), reconciling"
	NoDriftDetected = "no drift detected between %s (trigger table) and %s (change table), skipping"
	AuditCompleted  = "audit completed in %vs"
	PurgeCompleted  = "purge completed in %vs"
)
