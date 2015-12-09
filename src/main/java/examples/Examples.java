package examples;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.java.spi.cluster.impl.jgroups.JGroupsClusterManager;
import org.jgroups.JChannel;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class Examples {

  public void example1() {
    ClusterManager mgr = new JGroupsClusterManager();
    VertxOptions options = new VertxOptions().setClusterManager(mgr);
    Vertx.clusteredVertx(options, res -> {
      if (res.succeeded()) {
        Vertx vertx = res.result();
      } else {
        // failed!
      }
    });
  }

  public void example2(JChannel channel) {
    ClusterManager mgr = new JGroupsClusterManager(channel);
    VertxOptions options = new VertxOptions().setClusterManager(mgr);
    Vertx.clusteredVertx(options, res -> {
      if (res.succeeded()) {
        Vertx vertx = res.result();
      } else {
        // failed!
      }
    });

    // Don't forget to close the channel when done.
  }
}
