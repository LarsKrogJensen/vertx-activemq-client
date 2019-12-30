package se.lars.client;

import io.reactivex.Completable;
import io.reactivex.Observable;
import io.reactivex.subjects.PublishSubject;
import io.vertx.reactivex.core.Vertx;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.transport.TransportListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.vertx.reactivex.core.RxHelper.scheduler;

public class ActiveMqConsumer{
  private static final Logger log = LoggerFactory.getLogger(ActiveMqConsumer.class);
  private final Vertx vertx;
  private final ActiveMqConsumerOptions options;
  private final PublishSubject<Message> subject = PublishSubject.create();
  private final AtomicBoolean connected = new AtomicBoolean(false);

  private Connection connection;
  private Session session;

  private ActiveMqConsumer(Vertx vertx, ActiveMqConsumerOptions options) {
    this.vertx = Objects.requireNonNull(vertx);
    this.options = Objects.requireNonNull(options);
  }

  public static ActiveMqConsumer create(Vertx vertx) {
    return new ActiveMqConsumer(vertx, new ActiveMqConsumerOptions());
  }

  public static ActiveMqConsumer create(Vertx vertx, ActiveMqConsumerOptions options) {
    return new ActiveMqConsumer(vertx, options);
  }

  public boolean isConnected() {
    return connected.get();
  }

  /**
   * Connects with the url and retries until succeeded signals with the Completable
   */
  public Completable connect(String baseUrl) {
    Objects.requireNonNull(baseUrl);

    String url = options.formatUrl(baseUrl);
    ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(url);
    cf.setMaxThreadPoolSize(1); // use as few threads as possible
    cf.setOptimizeAcknowledge(true); // reduce server chatiness, might cause duplicate messages on reconnect - imptove performance
    cf.setAlwaysSessionAsync(false); // use transport thread to deliver message, i.e. not via executor
    cf.setExceptionListener(this::onException);

    return vertx.rxExecuteBlocking(promise -> {
      try {
        log.info("Trying to connect {}", url);
        connection = cf.createConnection();
        ((ActiveMQConnection)connection).addTransportListener(new ConnectionListener(connected));
        connection.start();
        connected.set(true);
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

  /**
   * create and attach to the topic
   */
  public ActiveMqConsumer topic(String topic) {
    Objects.requireNonNull(topic);
    return topic(topic, null);
  }

  /**
   * create and attach to the topic with selector
   */
  public ActiveMqConsumer topic(String topic, String selector) {
    try {
      session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Topic destination = session.createTopic(topic);
      MessageConsumer consumer = session.createConsumer(destination, selector);
      consumer.setMessageListener(subject::onNext);
    } catch (Exception e) {
      log.error("Failed to session and topic {}", topic, e);
      throw new RuntimeException(e);
    }

    return this;
  }

  /**
   * Returns hot observable to listen for a stream of messages
   */
  public Observable<Message> listen() {
    return subject.observeOn(scheduler(vertx.getOrCreateContext()));
  }

  public void close() throws JMSException {
    subject.onComplete();
    if (session != null) session.close();
    if (connection != null) connection.close();
  }

  private void onException(JMSException exception) {
    log.error("ActiveMq exception: {}", exception.getMessage());
  }

  private static class ConnectionListener implements TransportListener {
    private final AtomicBoolean connected;

    public ConnectionListener(AtomicBoolean connected) {
      this.connected = connected;
    }

    @Override
    public void onCommand(Object command) {

    }

    @Override
    public void onException(IOException error) {

    }

    @Override
    public void transportInterupted() {
      connected.set(false);
    }

    @Override
    public void transportResumed() {
      connected.set(true);
    }
  }
}
