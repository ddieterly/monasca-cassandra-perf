import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;

import org.joda.time.DateTime;
import org.joda.time.Seconds;

public class MPerfRead {

  private static final int SOCKET_TIMEOUT_MILLIS = 20;
  private static final String CASSANDRA_IP_ADDRESS = "127.0.0.1";

  private int numTimesToRead;
  private Cluster cluster = null;
  private final Session session ;

  public static void main(String[] args) {


    if (args.length != 1) {
      System.out.println("Usage: MPerfRead <num times to read>");
      System.exit(-1);
    }

    int numTimesToRead = Integer.parseInt(args[0]);

    MPerfRead mPerfRead = new MPerfRead(numTimesToRead);

    DateTime start = DateTime.now();

    mPerfRead.run();

    DateTime end = DateTime.now();
    int seconds  = Seconds.secondsBetween(start, end).getSeconds();


    System.out.format("Elapsed seconds for all reads to complete: %d%n", seconds);
    System.out.format("Reads per sec: %d%n", numTimesToRead / seconds);
    System.out.println("Finished!");
  }


  private MPerfRead(int numTimesToRead) {

    this.numTimesToRead = numTimesToRead;
    cluster =
        cluster.builder().addContactPoint(CASSANDRA_IP_ADDRESS)
            .withSocketOptions(new SocketOptions().setConnectTimeoutMillis(SOCKET_TIMEOUT_MILLIS))
            .build();

    this.session = cluster.connect();
  }

  private void run() {

    for (int i = 0; i < this.numTimesToRead; i++) {
      ResultSet resultSet = this.session.execute("select * from monasca.measurements limit 100");
    }
  }

}
