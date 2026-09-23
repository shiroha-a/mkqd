package mkqd

// Version is the mkqd release this binary was built from. Release
// builds overwrite it at link time; the constant here tracks the
// in-development version.
var Version = "0.2.0"

// UserAgent is the default User-Agent mkqd sends on outbound HTTP,
// before any per-executor override.
func UserAgent() string { return "mkqd/" + Version }
