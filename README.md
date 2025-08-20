# Spanner Chaos Testing

This sample project shows how either gRPC interceptors or a mock Spanner server can be used to add
tests for specific Spanner error conditions.

## gRPC Interceptors (Simple)

This is the easiest way to inject (random) error conditions into end-to-end tests with Spanner.
gRPC interceptors can be added to a Spanner client. These can inject errors or in other ways modify
the results that are being returned to the application.

See [SpannerInterceptorTest.java](src/test/java/com/google/cloud/spanner/chaos/SpannerInterceptorTest.java)
for a few examples for how to add interceptors to a Spanner client.

gRPC interceptors can be used both with a normal Spanner instance and with the Spanner emulator.

## Mock Spanner server (Advanced)

The Spanner client library contains a mock Spanner server. This server is used internally by
the client library for tests, but can also be used for application testing.

The mock Spanner server does not replicate all features in Spanner, and can be difficult to set up
and use. It does however allow you to test more advanced scenarios, like returning specific errors
for specific SQL statements, or adding (random) delays to specific RPCs.

See [SpannerMockServerTest.java](src/test/java/com/google/cloud/spanner/chaos/SpannerMockServerTest.java)
for a few examples for how to use this mock server.

It is recommended to go through the various test cases in the Spanner client library to get a better
view of how to use the mock Spanner server for more advanced use cases. Search for subclasses of
[AbstractMockServerTest.java](https://github.com/googleapis/java-spanner/blob/main/google-cloud-spanner/src/test/java/com/google/cloud/spanner/connection/AbstractMockServerTest.java)
to find examples.
