package se.lars.client;

import org.apache.activemq.broker.BrokerService;

import java.io.Closeable;

public class TestBroker {
  public static Closeable startBroker(int port) {
    try {
      BrokerService broker = new BrokerService();

      // configure the broker
      broker.setPersistent(false);
      broker.setUseJmx(false);
      broker.addConnector("tcp://localhost:" + port);
      broker.start();

      return () -> {
        try {
          broker.stop();
        } catch (Exception ignored) {
        }
      };
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
