package com.google.cloud.spanner.chaos;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.NoCredentials;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.MockSpannerServiceImpl;
import com.google.cloud.spanner.MockSpannerServiceImpl.SimulatedExecutionTime;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.net.InetSocketAddress;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SpannerMockServerTest {
  private static Server server;
  private static MockSpannerServiceImpl mockSpannerService;
  private static Spanner spanner;

  @BeforeClass
  public static void setupMockServer() throws Exception {
    mockSpannerService = new MockSpannerServiceImpl();
    // The mock Spanner server by default aborts 0.001 of all transactions.
    // This test sets this probability to zero to prevent random aborts of read/write transactions.
    mockSpannerService.setAbortProbability(0.0);

    // Start a gRPC server on a random port on localhost.
    InetSocketAddress address = new InetSocketAddress("localhost", 0);
    server = NettyServerBuilder.forAddress(address).addService(mockSpannerService).build().start();

    // Create a Spanner client that connects to the mock server.
    spanner =
        SpannerOptions.newBuilder()
            .setProjectId("fake-project")
            .setHost(String.format("http://localhost:%d", server.getPort()))
            // The mock server does not use TLS, so we need to instruct the Spanner client to use
            // plain text communication.
            .setChannelConfigurator(ManagedChannelBuilder::usePlaintext)
            .setCredentials(NoCredentials.getInstance())
            .build()
            .getService();
  }

  @AfterClass
  public static void cleanupMockServer() throws Exception {
    server.shutdown();
    server.awaitTermination();
  }

  @Test
  public void testSimpleQuery() {
    // We need to tell the mock Spanner server what result it should return for any SQL statement
    // that we execute.
    mockSpannerService.putStatementResult(
        StatementResult.query(
            Statement.of("select * from my_table"),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    ResultSetMetadata.newBuilder()
                        .setRowType(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("id")
                                        .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                                        .build())
                                .addFields(
                                    Field.newBuilder()
                                        .setName("name")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .build())
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("1").build())
                        .addValues(Value.newBuilder().setStringValue("One").build())
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("2").build())
                        .addValues(Value.newBuilder().setStringValue("Two").build())
                        .build())
                .build()));

    DatabaseClient client =
        spanner.getDatabaseClient(DatabaseId.of("fake-project", "fake-instance", "fake-database"));
    try (ResultSet resultSet =
        client.singleUse().executeQuery(Statement.of("select * from my_table"))) {
      assertTrue(resultSet.next());
      assertEquals(1, resultSet.getLong(0));
      assertEquals("One", resultSet.getString(1));
      assertTrue(resultSet.next());
      assertEquals(2, resultSet.getLong(0));
      assertEquals("Two", resultSet.getString(1));
      assertFalse(resultSet.next());
    }
  }

  @Test
  public void testQueryError() {
    // We can also tell the mock server to return an error for a specific query.
    mockSpannerService.putStatementResult(
        StatementResult.exception(
            Statement.of("select * from my_table"),
            Status.PERMISSION_DENIED
                .augmentDescription("User is not allowed to query my_table")
                .asRuntimeException()));

    DatabaseClient client =
        spanner.getDatabaseClient(DatabaseId.of("fake-project", "fake-instance", "fake-database"));
    try (ResultSet resultSet =
        client.singleUse().executeQuery(Statement.of("select * from my_table"))) {
      // Note that the Spanner client defers the actual query execution until the first call to
      // ResultSet#next(). This means that the executeQuery(..) method succeeds, while the
      // ResultSet#next() method fails with PERMISSION_DENIED.
      SpannerException exception = assertThrows(SpannerException.class, resultSet::next);
      assertEquals(ErrorCode.PERMISSION_DENIED, exception.getErrorCode());
    }
  }

  @Test
  public void testCommitError() {
    // We can also tell the mock server to return an error for a specific RPC.
    mockSpannerService.setCommitExecutionTime(
        SimulatedExecutionTime.ofStickyException(
            Status.ALREADY_EXISTS
                .augmentDescription("A row with ID 1 already exists")
                .asRuntimeException()));

    DatabaseClient client =
        spanner.getDatabaseClient(DatabaseId.of("fake-project", "fake-instance", "fake-database"));
    SpannerException exception =
        assertThrows(
            SpannerException.class,
            () ->
                client
                    .readWriteTransaction()
                    .run(
                        transaction -> {
                          transaction.buffer(
                              Mutation.newInsertBuilder("my_table")
                                  .set("id")
                                  .to(1L)
                                  .set("name")
                                  .to("One")
                                  .build());
                          return null;
                        }));
    assertEquals(ErrorCode.ALREADY_EXISTS, exception.getErrorCode());
  }
}
