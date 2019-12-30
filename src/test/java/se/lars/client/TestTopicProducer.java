package se.lars.client;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

public class TestTopicProducer {
  private Connection connection;
  private Session session;
  private final MessageProducer producer;

  public static TestTopicProducer createProducer() throws Exception {
    return new TestTopicProducer(61616, "topic");
  }

  public static TestTopicProducer createProducer(int port) throws Exception {
    return new TestTopicProducer(port, "topic");
  }

  public TestTopicProducer(int port, String topic) throws Exception {
    ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("tcp://localhost:" + port);
    cf.setCopyMessageOnSend(false);

    connection = cf.createConnection();
    connection.start();
    session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    var destionation = session.createTopic(topic);

    producer = session.createProducer(destionation);
    producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
  }

  public void send(String message) throws Exception {
    TextMessage textMessage = session.createTextMessage(message);
    producer.send(textMessage);
  }

  public void close() throws Exception {
    if (session != null) session.close();
    if (connection != null) connection.close();
  }
}
