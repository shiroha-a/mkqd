package mkqd

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A type the docs mention but no package implements must say so, rather
// than point at an import that would not compile. Nothing is in that
// state right now, so the branch is exercised through the table.
func TestUnknownExecutorError_DistinguishesPlannedFromUnlinked(t *testing.T) {
	plannedExecutors["not-yet"] = struct{}{}
	t.Cleanup(func() { delete(plannedExecutors, "not-yet") })

	require.ErrorContains(t, unknownExecutorError("not-yet"), "not implemented yet")
	require.NotContains(t, unknownExecutorError("not-yet").Error(), "import _",
		"an unimplemented type must not suggest an import path")

	for _, typ := range []string{"http", "webhook", "activitypub_deliver"} {
		require.ErrorContains(t, unknownExecutorError(typ), "import _")
		require.ErrorContains(t, unknownExecutorError(typ), builtinExecutorPackages[typ])
	}

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
