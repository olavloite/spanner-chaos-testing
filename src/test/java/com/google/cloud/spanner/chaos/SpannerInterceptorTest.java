package com.google.cloud.spanner.chaos;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.common.collect.ImmutableList;
import com.google.spanner.v1.SpannerGrpc;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** This test class shows how to use gRPC interceptors to inject (random) errors in tests. */
@RunWith(JUnit4.class)
public class SpannerInterceptorTest {

  @Test
  public void testDeadlineExceeded() {
    // Create an interceptor that throws a DEADLINE_EXCEEDED error whenever the ExecuteStreamingSql
    // RPC is invoked.
    ClientInterceptor interceptor =
        new ClientInterceptor() {
          public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
              MethodDescriptor<ReqT, RespT> methodDescriptor,
              CallOptions callOptions,
              Channel channel) {
            // This test only injects errors for ExecuteStreamingSql.
            // Note that MethodDescriptor does not override the equals(..) method, so comparing
            // MethodDescriptors directly is not supported.
            // Instead, the comparison should be done based on the full method name.
            if (!methodDescriptor
                .getFullMethodName()
                .equals(SpannerGrpc.getExecuteStreamingSqlMethod().getFullMethodName())) {
              return channel.newCall(methodDescriptor, callOptions);
            }

            // This interceptor always throws an exception. Additional logic can be added to only
            // throw an exception if certain conditions are met, or at random.
            throw Status.DEADLINE_EXCEEDED
                .augmentDescription("INJECTED BY TEST")
                .asRuntimeException();
          }
        };

    Spanner spanner =
        SpannerOptions.newBuilder()
            .setProjectId("my-project")
            .setInterceptorProvider(() -> ImmutableList.of(interceptor))
            .build()
            .getService();
    DatabaseClient client =
        spanner.getDatabaseClient(
            DatabaseId.of("my-project", "my-instance", "my-database"));
    try (ResultSet resultSet =
        client.singleUse().executeQuery(Statement.of("select * from my_table limit 10"))) {
      while (resultSet.next()) {
        System.out.println(resultSet.getCurrentRowAsStruct());
      }
      fail("Should have thrown an exception");
    } catch (SpannerException spannerException) {
      // This query should throw a DEADLINE_EXCEEDED error.
      assertEquals(ErrorCode.DEADLINE_EXCEEDED, spannerException.getErrorCode());
    }
  }

  @Test
  public void testAbortedTransaction() {
    // Create an interceptor that randomly throws an Aborted exception for the ExecuteStreamingSql,
    // ExecuteSql, and Commit RPCs.
    ClientInterceptor interceptor =
        new ClientInterceptor() {
          public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
              MethodDescriptor<ReqT, RespT> methodDescriptor,
              CallOptions callOptions,
              Channel channel) {
            if (!(methodDescriptor
                    .getFullMethodName()
                    .equals(SpannerGrpc.getExecuteStreamingSqlMethod().getFullMethodName())
                || methodDescriptor
                    .getFullMethodName()
                    .equals(SpannerGrpc.getExecuteSqlMethod().getFullMethodName())
                || methodDescriptor
                    .getFullMethodName()
                    .equals(SpannerGrpc.getCommitMethod().getFullMethodName()))) {
              return channel.newCall(methodDescriptor, callOptions);
            }

            if (ThreadLocalRandom.current().nextBoolean()) {
              throw Status.ABORTED.augmentDescription("INJECTED BY TEST").asRuntimeException();
            }
            return channel.newCall(methodDescriptor, callOptions);
          }
        };

    Spanner spanner =
        SpannerOptions.newBuilder()
            .setProjectId("my-project")
            .setInterceptorProvider(() -> ImmutableList.of(interceptor))
            .build()
            .getService();
    DatabaseClient client =
        spanner.getDatabaseClient(
            DatabaseId.of("my-project", "my-instance", "my-database"));

    // Verify that running a transaction where statements can randomly be aborted still eventually
    // succeeds.
    AtomicInteger attempts = new AtomicInteger(0);
    client
        .readWriteTransaction()
        .run(
            transaction -> {
              attempts.incrementAndGet();
              try (ResultSet resultSet =
                  transaction.executeQuery(Statement.of("select * from my_table limit 10"))) {
                while (resultSet.next()) {
                  System.out.println(resultSet.getCurrentRowAsStruct());
                }
              }
              return transaction.executeUpdate(
                  Statement.newBuilder("update my_table set value=@value where id=@id")
                      .bind("value")
                      .to(ThreadLocalRandom.current().nextDouble())
                      .bind("id")
                      .to(ThreadLocalRandom.current().nextInt())
                      .build());
            });
    System.out.printf("Transaction attempts before success: %d\n", attempts.get());
  }
}
