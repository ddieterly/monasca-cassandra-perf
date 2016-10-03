

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;

import org.apache.commons.codec.digest.DigestUtils;
import org.joda.time.DateTime;
import org.joda.time.Seconds;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicInteger;

public class MPerf {

  private static final int NUMBER_OF_MEASUREMENTS_TO_INSERT = 150000;
  private static final int NUMBER_PER_BATCH = 15;
  private static final int NUMBER_OF_UNIQUE_METRICS = 1000;
  private static final int SOCKET_TIMEOUT_MILLIS = 20;
  private static final java.sql.Timestamp BUCKET_START_TIMESTAMP = new java.sql.Timestamp(0);

  private static final String metricNamePrefix = "metric_";
  private static final String REGION = "region_1";
  private static final String TENANT_ID = "tenant_1";


  private static final HashSet<String>
      dimensionKeysSet =
      new HashSet<String>(Arrays.asList("service", "host", "cloud"));

  private static final HashMap<String, String> dimensionKeysValuesMap = new HashMap<String, String>();
  static {
    dimensionKeysValuesMap.put("service", "monitoring");
    dimensionKeysValuesMap.put("host", "localhost");
    dimensionKeysValuesMap.put("cloud", "cloud_3");
  }

  private static final String dimensionHashString = "cloudcloud_3hostlocalhostservicemonitoring";
  private static final String valueMetaPrefix = "value_meta_";


  public static void main(String[] args) {


    MPerf mPerf = new MPerf();

    DateTime start = DateTime.now();

    mPerf.runPerfTest();

    DateTime end = DateTime.now();

    Seconds seconds  = Seconds.secondsBetween(start, end);

    System.out.println("elapsed time for async batches to complete: " + new Integer(seconds.getSeconds()).toString());
    System.out.println("measurements per sec: " + new Integer(NUMBER_OF_MEASUREMENTS_TO_INSERT / seconds.getSeconds()).toString());

    System.out.println("Finished!");

  }

  private void runPerfTest() {

    Cluster cluster = null;

    try {

      cluster =
          cluster.builder().addContactPoint("127.0.0.1")
              .withSocketOptions(new SocketOptions().setConnectTimeoutMillis(SOCKET_TIMEOUT_MILLIS))
              .build();
      Session session = cluster.connect();

      PreparedStatement
          measurementsInsertStmt =
          session.prepare(
              "insert into monasca.measurements (region, tenant_id, bucket_start, metric_id, "
              + "time_stamp, value, value_meta) values (?, ?, ?, ?, ?, ?, ?)");

      PreparedStatement
          metricsInsertStmt =
          session.prepare(
              "INSERT INTO monasca.metrics (region, tenant_id, bucket_start, created_at, "
              + "updated_at, metric_id, metric_name, dimension_keys_set, "
              + "dimension_keys_values_map) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");

      Calendar calendar = Calendar.getInstance();
      int metricCount = 0;
      HashMap<String, java.sql.Timestamp> metricCreateDateMap = new HashMap<>();

      final AtomicInteger atomicSuccessCount = new AtomicInteger();
      final AtomicInteger atomicErrorCount = new AtomicInteger();

      MyFutureCallback myFutureCallback = new MyFutureCallback(atomicSuccessCount, atomicErrorCount);

      for (int i = 0; i < NUMBER_OF_MEASUREMENTS_TO_INSERT / NUMBER_PER_BATCH; i++) {

        BatchStatement batchStatement = new BatchStatement();

        for (int j = 0; j < NUMBER_PER_BATCH; j++) {

          String metricNameSuffix = new Integer(metricCount % NUMBER_OF_UNIQUE_METRICS).toString();
          String metricName = metricNamePrefix + metricNameSuffix;
          String metricIdHashString = REGION + TENANT_ID + metricName + dimensionHashString;
          ByteBuffer metricIdSha1HashByteBuffer = ByteBuffer.wrap(DigestUtils.sha(metricIdHashString));
          java.sql.Timestamp nowTimeStamp = new java.sql.Timestamp(calendar.getTime().getTime());
          java.sql.Timestamp createdAt = null;
          java.sql.Timestamp updatedAt = nowTimeStamp;
          if (metricCreateDateMap.containsKey(metricName)) {
            createdAt = metricCreateDateMap.get(metricName);
          } else {
            createdAt = nowTimeStamp;
          }

          BoundStatement
              measurmentsBoundStmt =
              measurementsInsertStmt
                  .bind(TENANT_ID, REGION, BUCKET_START_TIMESTAMP, metricIdSha1HashByteBuffer, updatedAt, (float) metricCount,
                        metricNameSuffix);

//          batchStatement.add(measurmentsBoundStmt);
          ResultSetFuture future1 = session.executeAsync(measurmentsBoundStmt);
          Futures.addCallback(future1, myFutureCallback);

          BoundStatement
              metricBoundStmt =
              metricsInsertStmt
                  .bind(REGION, TENANT_ID,BUCKET_START_TIMESTAMP, createdAt, updatedAt, metricIdSha1HashByteBuffer, metricName, dimensionKeysSet, dimensionKeysValuesMap);


//          batchStatement.add(metricBoundStmt);

//          ResultSetFuture future = session.executeAsync(batchStatement);
          ResultSetFuture future2 = session.executeAsync(metricBoundStmt);
          Futures.addCallback(future2, myFutureCallback);

          metricCount++;

        }

      }

      while (myFutureCallback.atomicErrorCount.get() + myFutureCallback.atomicSuccessCount.get() < NUMBER_OF_MEASUREMENTS_TO_INSERT) {

        try {
          System.out.println("Sleeping for 5 seconds...");
          Thread.sleep(5000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }

      }

    } finally {

      if (cluster != null) {
        cluster.close();
      }

    }
  }

  private static class MyFutureCallback implements FutureCallback<ResultSet> {

     AtomicInteger atomicSuccessCount = null;
     AtomicInteger atomicErrorCount = null;

    MyFutureCallback (AtomicInteger atomicSuccessCount, AtomicInteger atomicErrorCount) {

      this.atomicSuccessCount = atomicSuccessCount;
      this.atomicErrorCount = atomicErrorCount;
    }

    @Override
    public void onSuccess(ResultSet result) {
      this.atomicSuccessCount.getAndIncrement();

    }

    @Override
    public void onFailure(Throwable t) {
      this.atomicErrorCount.getAndIncrement();

    }
  }

}
