package mkqd

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A type the docs mention but no package implements must say so, rather
// than point at an import that would not compile.
func TestUnknownExecutorError_DistinguishesPlannedFromUnlinked(t *testing.T) {
	require.ErrorContains(t, unknownExecutorError("activitypub_deliver"), "not implemented yet")
	require.NotContains(t, unknownExecutorError("activitypub_deliver").Error(), "import _",
		"an unimplemented type must not suggest an import path")

	require.ErrorContains(t, unknownExecutorError("http"),
		"github.com/shiroha-a/mkqd/executor/httpexec")
	require.ErrorContains(t, unknownExecutorError("http"), "import _")

	require.ErrorContains(t, unknownExecutorError("nonsense"), "unknown executor type")
}

// Every package named as the home of a built-in executor must exist, or
// the hint sends the operator to an import that will not compile.
func TestBuiltinExecutorPackages_AreRealPackages(t *testing.T) {
	for typ, pkg := range builtinExecutorPackages {
		require.NotContains(t, plannedExecutors, typ,
			"%s cannot be both planned and linkable", typ)
		require.NotEmpty(t, pkg)
	}
}
