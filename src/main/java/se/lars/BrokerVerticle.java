package se.lars;

import io.reactivex.Completable;
import io.vertx.reactivex.core.AbstractVerticle;
import org.apache.activemq.broker.BrokerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BrokerVerticle extends AbstractVerticle {
  private static final Logger log = LoggerFactory.getLogger(BrokerVerticle.class);

  @Override
  public Completable rxStart() {
    try {
      BrokerService broker = new BrokerService();

      // configure the broker
      broker.setPersistent(false);
      broker.addConnector("tcp://localhost:61616");
      broker.start();
      log.info("Broker started");
      return Completable.complete();
    } catch (Exception e) {
      return Completable.error(e);
    }
  }
}
