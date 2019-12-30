package se.lars;

import io.reactivex.Completable;
import io.reactivex.disposables.Disposable;
import io.vertx.core.Future;
import io.vertx.reactivex.core.AbstractVerticle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.lars.client.ActiveMqConsumer;

import javax.jms.ExceptionListener;
import javax.jms.JMSException;

public class ConsumerVerticle extends AbstractVerticle implements ExceptionListener {
  private static final Logger log = LoggerFactory.getLogger(ConsumerVerticle.class);
  private ActiveMqConsumer client;
  private Disposable subscription;

  @Override
  public Completable rxStart() {
    client = ActiveMqConsumer.create(vertx);
    return client.connect("tcp://localhost:61617")
      .doOnComplete(this::startConsumer);
  }

  @Override
  public void stop(Future<Void> stopFuture) throws Exception {
    if (subscription != null && !subscription.isDisposed()) {
      subscription.dispose();
    }
    if (client != null) {
      client.close();
    }
  }

  private void startConsumer() {
    subscription = client.topic("time-tick")
      .listen()
      .subscribe(
        msg -> log.info("Message: {}", msg),
        ex -> log.error("Oh nooes", ex)
      );
  }

  @Override
  public void onException(JMSException exception) {
    log.error("ActiveMQ error", exception);
  }
}
