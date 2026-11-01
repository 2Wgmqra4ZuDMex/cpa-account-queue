# Provenance

This is an independent implementation of a local account admission controller.
No source code or release binaries from `tsunheimat/cpa-courrency-plugin` have
been copied into this project. Its public design and the incident observed with
v0.1.8 motivated the lifecycle regression tests. The production incident's root
cause has **not** yet been reproduced or established.

Reference reviewed: upstream commit
`aa07db00a0bd5420936898f360f88a0ca79386bc`.

The native ABI adapter implements CLIProxyAPI's public C ABI 1 / JSON schema 7
wire contract. Compatibility reference: CLIProxyAPI commit `dbb7062`, v8.0.48.
The adapter deliberately does not import the full CPA SDK or register any
stream-chunk interceptor. ABI compatibility must be tested against a real host
before a production release.
