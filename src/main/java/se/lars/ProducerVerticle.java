package se.lars;

import io.reactivex.Completable;
import io.vertx.reactivex.core.AbstractVerticle;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.time.OffsetDateTime;

public class ProducerVerticle extends AbstractVerticle {
  private static final Logger log = LoggerFactory.getLogger(ProducerVerticle.class);

  @Override
  public Completable rxStart() {

    try {
      ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("tcp://localhost:61616");
      cf.setCopyMessageOnSend(false);
//      cf.setAlwaysSyncSend();
      var connection = cf.createConnection();
      connection.start();
      var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      var destionation = session.createTopic("time-tick");

      var producer = session.createProducer(destionation);
      producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

      vertx.setPeriodic(1000, __ -> {
        try {
          TextMessage textMessage = session.createTextMessage(OffsetDateTime.now().toString());
          producer.send(textMessage);
        } catch (JMSException e) {
          log.error("Failed to produce message");
        }
      });
    } catch (JMSException e) {
      return Completable.error(e);
    }
    log.info("Producer started");
    return Completable.complete();
  }
}
