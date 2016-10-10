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
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class MPerfThreaded {

  private int numThreads = 0;
  List<MPerfRunnable> mPerfRunnableArrayList = new ArrayList<>();

  private static int NUMBER_OF_MEASUREMENTS_TO_INSERT = 0;
  private static final int NUMBER_OF_UNIQUE_METRICS = 1000;
  private static final java.sql.Timestamp BUCKET_START_TIMESTAMP = new java.sql.Timestamp(0);
  private static final int SOCKET_TIMEOUT_MILLIS = 20;

  private static final String metricNamePrefix = "metric_";
  private static final String REGION = "region_1";
  private static final String TENANT_ID = "tenant_1";

  private static final String dimensionHashString = "cloudcloud_3hostlocalhostservicemonitoring";
  private static final String valueMetaPrefix = "value_meta_";

  private static final Logger logger = LoggerFactory.getLogger(MPerfThreaded.class);

  public static void main(String[] args) {

    if (args.length != 2) {
      System.out.println("Usage: MPerfThreaded <num threads> <num per thread>");
      System.exit(-1);
    }

    int numThreads = Integer.parseInt(args[0]);
    NUMBER_OF_MEASUREMENTS_TO_INSERT = Integer.parseInt(args[1]);

    logger.debug("Number of threads: " + numThreads);

    MPerfThreaded mPerfThreaded = new MPerfThreaded(numThreads);

    DateTime start = DateTime.now();

    int totalSuccessCnt = mPerfThreaded.runTests();

    logger.debug("Total success count: " + totalSuccessCnt);

    DateTime end = DateTime.now();

    int seconds  = Seconds.secondsBetween(start, end).getSeconds();

    System.out.println("elapsed time for all async batches to complete: " + new Integer(seconds).toString());
    System.out.println("measurements per sec: " + new Integer(totalSuccessCnt / seconds).toString());
    System.out.println("Finished!");
  }

  public MPerfThreaded (int numThreads) {

    this.numThreads = numThreads;

    for (int i = 0; i < numThreads; i++) {

      MPerfRunnable mPerfRunnable = new MPerfRunnable();
      mPerfRunnableArrayList.add(mPerfRunnable);
    }

  }

  private int runTests() {


    for (int i = 0; i < numThreads; i++) {

      MPerfRunnable mPerfRunnable = mPerfRunnableArrayList.get(i);
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

        successCnt += mPerfRunnableArrayList.get(i).successCnt;
        errorCnt += mPerfRunnableArrayList.get(i).errorCnt;

      }

      logger.debug("successCnt: " + successCnt);
      logger.debug("errorCnt: " + errorCnt);

      if (successCnt + errorCnt == NUMBER_OF_MEASUREMENTS_TO_INSERT * numThreads) {

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

  private class MPerfRunnable implements Runnable {

    int successCnt = 0;
    int errorCnt = 0;
    boolean done = false;
    private Cluster cluster = null;
    private Session session = null;
    private PreparedStatement measurementsInsertStmt = null;


    MPerfRunnable() {

      cluster =
          cluster.builder().addContactPoint("127.0.0.1")
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
        HashMap<String, Timestamp> metricCreateDateMap = new HashMap<>();

        List<MyFutureCallbackInt> myFutureCallbackList = new LinkedList<>();

        for (int i = 0; i < NUMBER_OF_MEASUREMENTS_TO_INSERT; i++) {

          String metricNameSuffix = new Integer(i % NUMBER_OF_UNIQUE_METRICS).toString();
          String metricName = metricNamePrefix + metricNameSuffix;
          String metricIdHashString = REGION + TENANT_ID + metricName + dimensionHashString;
          ByteBuffer metricIdSha1HashByteBuffer = ByteBuffer.wrap(DigestUtils.sha(metricIdHashString));
          java.sql.Timestamp nowTimeStamp = new java.sql.Timestamp(calendar.getTime().getTime());
//          java.sql.Timestamp createdAt = null;
          java.sql.Timestamp updatedAt = nowTimeStamp;
//          if (metricCreateDateMap.containsKey(metricName)) {
//            createdAt = metricCreateDateMap.get(metricName);
//          } else {
//            createdAt = nowTimeStamp;
//          }

          BoundStatement
              measurmentsBoundStmt =
              measurementsInsertStmt
                  .bind(TENANT_ID, REGION, BUCKET_START_TIMESTAMP, metricIdSha1HashByteBuffer,
                        updatedAt, (float) i, metricNameSuffix);

          MyFutureCallbackInt myFutureCallback1 = new MyFutureCallbackInt();

          ResultSetFuture future1 = session.executeAsync(measurmentsBoundStmt);
          Futures.addCallback(future1, myFutureCallback1);
          myFutureCallbackList.add(myFutureCallback1);

        }

        while (!done) {

          successCnt = 0;
          errorCnt = 0;

          for (MyFutureCallbackInt myFutureCallback : myFutureCallbackList) {

            successCnt += myFutureCallback.successCount;
            errorCnt += myFutureCallback.errorCount;

          }

          logger.debug("successCnt: " + successCnt);
          logger.debug("errorCnt: " + errorCnt);

          if (successCnt + errorCnt == NUMBER_OF_MEASUREMENTS_TO_INSERT) {

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
