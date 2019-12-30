package se.lars;

import io.vertx.reactivex.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Application {
  private static final Logger log = LoggerFactory.getLogger(Application.class);

  public void start() {
    Vertx vertx = Vertx.vertx();

    vertx.rxDeployVerticle(new BrokerVerticle())
      .flatMap(__ -> vertx.rxDeployVerticle(new ProducerVerticle()))
      .flatMap(__ -> vertx.rxDeployVerticle(new ConsumerVerticle()))
      .subscribe(
        __ -> log.info("App started"),
        ex -> log.error("App failed to start", ex)
      );

  }
}
