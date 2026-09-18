package version

// Version is overridden at build time via -ldflags "-X .../internal/version.Version=<tag>".
var Version = "dev"
