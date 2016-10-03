
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class MPerf {


  public static void main(String[] args) {

    Cluster cluster = null;

    try {

//      org.apache.log4j.BasicConfigurator.configure();

      cluster = cluster.builder()
          .addContactPoint("127.0.0.1")
          .build();
      Session session = cluster.connect();

      ResultSet rs = session.execute("select * from monasca.measurements");
      Row row = rs.one();
      System.out.println(row.getString("value_meta"));

    } finally {

      if (cluster != null) cluster.close();

    }

    System.out.println("hello!");
  }


}
