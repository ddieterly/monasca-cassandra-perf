import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;

import org.apache.commons.codec.digest.DigestUtils;
import org.joda.time.DateTime;
import org.joda.time.Seconds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;

public class MPerfThreaded {

  private static final java.sql.Timestamp BUCKET_START_TIMESTAMP = new java.sql.Timestamp(0);
  private static final int SOCKET_TIMEOUT_MILLIS = 20;
  private static final String CASSANDRA_IP_ADDRESS = "127.0.0.1";

  private static final String metricNamePrefix = "metric_";
  private static final String REGION = "region_1";
  private static final String TENANT_ID = "tenant_1";
  private static final String dimensionHashString = "cloudcloud_3hostlocalhostservicemonitoring";

  private static final Logger logger = LoggerFactory.getLogger(MPerfThreaded.class);

  private final int numMeasurementsToInsert;
  private final int numThreads;
  private final List<MPerfRunnable> mPerfRunnableList;

  public static void main(String[] args) {

    if (args.length != 3) {
      System.out.println("Usage: MPerfThreaded <num threads> <num measurements per thread> <num unique metrics>");
      System.exit(-1);
    }

    int numThreads = Integer.parseInt(args[0]);
    int numMeasurementsToInsert = Integer.parseInt(args[1]);
    int numUniqueMetrics = Integer.parseInt(args[2]);

    logger.debug("Number of threads: {}", numThreads);
    logger.debug("Number of measurements to insert: {}", numMeasurementsToInsert);
    logger.debug("Number of unique metrics: {}");

    MPerfThreaded mPerfThreaded = new MPerfThreaded(numThreads, numMeasurementsToInsert, numUniqueMetrics);

    DateTime start = DateTime.now();

    int totalSuccessCnt = mPerfThreaded.runTests();

    DateTime end = DateTime.now();
    int seconds  = Seconds.secondsBetween(start, end).getSeconds();

    System.out.format("Total upsert success count: %d%n", totalSuccessCnt);
    System.out.format("Total upsert error count: %d%n", + (numMeasurementsToInsert * numThreads) - totalSuccessCnt);

    System.out.format("Elapsed seconds for all async upserts to complete: %d%n", seconds);
    System.out.format("Measurements upserted per sec: %d%n", totalSuccessCnt / seconds);
    System.out.println("Finished!");
  }

  public MPerfThreaded (int numThreads, int numMeasurementsToInsert, int numUniqueMetrics) {

    this.numThreads = numThreads;
    this.numMeasurementsToInsert = numMeasurementsToInsert;
    this.mPerfRunnableList = new ArrayList<>(numThreads);

    for (int i = 0; i < numThreads; i++) {

      mPerfRunnableList.add(new MPerfRunnable(numMeasurementsToInsert, numUniqueMetrics));

    }
  }

  private int runTests() {

    for (int i = 0; i < numThreads; i++) {

      MPerfRunnable mPerfRunnable = mPerfRunnableList.get(i);
      new Thread(mPerfRunnable).start();
    }

    logger.debug("Finished starting threads");

    int successCnt = 0;
    int errorCnt = 0;
    boolean done = false;

    while (!done) {

      successCnt = 0;
      errorCnt = 0;

      for (int i = 0; i < numThreads; i++) {

        successCnt += mPerfRunnableList.get(i).successCnt;
        errorCnt += mPerfRunnableList.get(i).errorCnt;

      }

      logger.debug("successCnt: {}", successCnt);
      logger.debug("errorCnt: {}", errorCnt);

      if (successCnt + errorCnt == numMeasurementsToInsert * numThreads) {

        logger.debug("Main thread is done");

        done = true;

      } else {

        try {

          logger.debug("Main thread is going to sleep");
          Thread.sleep(5000);

        } catch (InterruptedException e) {

          System.out.println("Caught InterruptedException");
          done = true;

        }
      }
    }

    return successCnt;

  }

  private static class MPerfRunnable implements Runnable {

    private final int numMeasurmentsToInsert;
    private final int numUniqueMetrics;

    int successCnt = 0;
    int errorCnt = 0;
    boolean done = false;

    private Cluster cluster = null;
    private final Session session ;
    private final PreparedStatement measurementsInsertStmt;


    MPerfRunnable(int numMeasurmentsToInsert, int numUniqueMetrics) {

      this.numMeasurmentsToInsert = numMeasurmentsToInsert;
      this.numUniqueMetrics = numUniqueMetrics;

       cluster =
          cluster.builder().addContactPoint(CASSANDRA_IP_ADDRESS)
              .withSocketOptions(new SocketOptions().setConnectTimeoutMillis(SOCKET_TIMEOUT_MILLIS))
              .build();

      this.session = cluster.connect();

      this.measurementsInsertStmt =
          session.prepare(
              "insert into monasca.measurements (region, tenant_id, bucket_start, metric_id, "
              + "time_stamp, value, value_meta) values (?, ?, ?, ?, ?, ?, ?)");
    }

    @Override
    public void run() {

      try {

        Calendar calendar = Calendar.getInstance();

        List<MyFutureCallbackInt> myFutureCallbackList = new LinkedList<>();

        for (int i = 0; i < numMeasurmentsToInsert; i++) {

          String metricNameSuffix = String.valueOf(i % numUniqueMetrics);
          String metricName = metricNamePrefix.concat(metricNameSuffix);
          String metricIdHashString = new StringBuilder(REGION).append(TENANT_ID).append(metricName).append(dimensionHashString).toString();
          ByteBuffer metricIdSha1HashByteBuffer = ByteBuffer.wrap(DigestUtils.sha(metricIdHashString));
          java.sql.Timestamp updatedAt = new java.sql.Timestamp(calendar.getTime().getTime());

          BoundStatement
              measurmentsBoundStmt =
              measurementsInsertStmt
                  .bind(TENANT_ID, REGION, BUCKET_START_TIMESTAMP, metricIdSha1HashByteBuffer,
                        updatedAt, (float) i, metricNameSuffix);

          MyFutureCallbackInt myFutureCallback = new MyFutureCallbackInt();

          ResultSetFuture future = session.executeAsync(measurmentsBoundStmt);
          Futures.addCallback(future, myFutureCallback);
          myFutureCallbackList.add(myFutureCallback);

        }

        while (!done) {

          successCnt = 0;
          errorCnt = 0;

          for (MyFutureCallbackInt myFutureCallback : myFutureCallbackList) {

            successCnt += myFutureCallback.successCount;
            errorCnt += myFutureCallback.errorCount;

          }

          logger.debug("successCnt: {}", successCnt);
          logger.debug("errorCnt: {}", errorCnt);

          if (successCnt + errorCnt == numMeasurmentsToInsert) {

            logger.debug("Thread is done");

            done = true;

          } else {

            try {

              logger.debug("Thread worker going to sleep");
              Thread.sleep(1000);

            } catch (InterruptedException e) {

              System.out.println("Caught InterruptedException");
              done = true;
            }
          }
        }

      } finally {

        if (cluster != null) {

          logger.debug("Closing cluster");
          cluster.close();

        }
      }
    }
  }

  private static class MyFutureCallbackInt implements FutureCallback<ResultSet> {

    int successCount = 0;
    int errorCount = 0;

    MyFutureCallbackInt() {

    }

    @Override
    public void onSuccess(ResultSet result) {
      this.successCount++;

    }

    @Override
    public void onFailure(Throwable t) {
      this.errorCount++;

    }
  }

}
