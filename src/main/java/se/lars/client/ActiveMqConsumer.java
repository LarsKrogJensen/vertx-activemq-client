package se.lars.client;

import io.reactivex.Completable;
import io.reactivex.Observable;
import io.reactivex.subjects.PublishSubject;
import io.vertx.reactivex.core.Vertx;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;

import static io.vertx.reactivex.core.RxHelper.scheduler;

public class ActiveMqConsumer implements ExceptionListener{
  private static final Logger log = LoggerFactory.getLogger(ActiveMqConsumer.class);
  private final Vertx vertx;
  private final ActiveMqConsumerOptions options;
  private final PublishSubject<Message> subject = PublishSubject.create();

  private Connection connection;
  private Session session;


  private ActiveMqConsumer(Vertx vertx, ActiveMqConsumerOptions options) {
    this.vertx = vertx;
    this.options = options;
  }

  public static ActiveMqConsumer create(Vertx vertx) {
    return new ActiveMqConsumer(vertx, new ActiveMqConsumerOptions());
  }

  public static ActiveMqConsumer create(Vertx vertx, ActiveMqConsumerOptions options) {
    return new ActiveMqConsumer(vertx, options);
  }

  public Completable connect(String baseUrl) {
    String url = options.formatUrl(baseUrl);
    ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(url);
    cf.setMaxThreadPoolSize(1);
    cf.setOptimizeAcknowledge(true);
    cf.setAlwaysSessionAsync(false);
    cf.setExceptionListener(this);

    return vertx.rxExecuteBlocking(promise -> {
      try {
        log.info("Trying to connect {}", url);
        connection = cf.createConnection();
        connection.start();
        promise.complete();
      } catch (JMSException e) {
        try {
          connection.close();
        } catch (JMSException ex) {
          log.error("Failed to close connection", ex);
        }
        promise.fail(e);
      }
    })
      .doOnError(e -> log.error("Failed to connect to {}, caused by {}", url, e.getMessage()))
      .retry()
      .ignoreElement();
  }

  public ActiveMqConsumer topic(String topic) {
    try {
      session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Topic destination = session.createTopic(topic);
      MessageConsumer consumer = session.createConsumer(destination);
      consumer.setMessageListener(subject::onNext);
    } catch (Exception e) {
      log.error("Failed to session and topic {}", topic, e);
      throw new RuntimeException(e);
    }

    return this;
  }

  public Observable<Message> listen() {
    return subject.observeOn(scheduler(vertx.getOrCreateContext()));
  }

  public void close() throws JMSException {
    session.close();
    connection.close();
    subject.onComplete();

  }

  @Override
  public void onException(JMSException exception) {
    log.error("ActiveMq exception: {}", exception.getMessage());
  }
}
