package se.lars.client

import io.reactivex.observers.TestObserver
import io.vertx.reactivex.core.Vertx
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import javax.jms.TextMessage
import java.time.Duration
import java.util.concurrent.ThreadLocalRandom

import static se.lars.client.TestBroker.startBroker
import static se.lars.client.TestTopicProducer.createProducer

class ActiveMqConsumerSpec extends Specification {
  @Delegate
  PollingConditions conditions = new PollingConditions(timeout: 2)

  Vertx vertx

  def setup() {
    vertx = Vertx.vertx()
  }

  def cleanup() {
    vertx.close()
  }

  def "should connect and consume messages"() {
    given:
    def port = ThreadLocalRandom.current().nextInt(1024, 10_000)
    def broker = startBroker(port)
    def producer = createProducer(port)
    def consumer = ActiveMqConsumer.create(vertx)
    def subscriber = new TestObserver()

    when:
    consumer.connect("tcp://localhost:$port").blockingGet()
    consumer
      .topic("topic")
      .listen()
      .ofType(TextMessage)
      .map { it.text }
      .subscribe(subscriber)

    and:
    producer.send("a")
    producer.send("b")

    then:
    subscriber.awaitCount(2)
    subscriber.assertValues("a", "b")

    when:
    consumer.close()

    then:
    subscriber.awaitTerminalEvent()
    subscriber.assertComplete()

    cleanup:
    producer.close();
    broker.close()
  }

  def "should connect and consume messages with selector"() {
    given:
    def port = ThreadLocalRandom.current().nextInt(1024, 10_000)
    def broker = startBroker(port)
    def producer = createProducer(port)
    def consumer = ActiveMqConsumer.create(vertx)
    def subscriber = new TestObserver()

    when:
    consumer.connect("tcp://localhost:$port").blockingGet()
    consumer
      .topic("topic", "JMSType in ('x', 'z')")
      .listen()
      .ofType(TextMessage)
      .map { it.text }
      .subscribe(subscriber)

    and:
    producer.send("a", "x")
    producer.send("b", "y")
    producer.send("c", "k")
    producer.send("d", "z")

    then:
    subscriber.awaitCount(2)
    subscriber.assertValues("a", "d")

    when:
    consumer.close()

    then:
    subscriber.awaitTerminalEvent()
    subscriber.assertComplete()

    cleanup:
    producer.close();
    broker.close()

  }

  def "should reconnect and continue consume messages"() {
    given:
    def port = ThreadLocalRandom.current().nextInt(1024, 10_000)
    def broker = startBroker(port)
    def producer = createProducer(port)
    def consumer = ActiveMqConsumer.create(vertx)
    def subscriber = new TestObserver()

    when:
    consumer.connect("tcp://localhost:$port").blockingGet()
    consumer
      .topic("topic")
      .listen()
      .ofType(TextMessage)
      .map { it.text }
      .subscribe(subscriber)

    and:
    producer.send("a")
    producer.send("b")

    then:
    subscriber.awaitCount(2)
    subscriber.assertValues("a", "b")

    when: "close broker"
    broker.close()
    producer.close()

    then: "await connection is closed"
    eventually {
      assert !consumer.connected
    }

    when: "resstart broker"
    broker = startBroker(port)
    producer = createProducer(port)

    then: "consumer will eventually be reconnected"
    eventually {
      assert consumer.connected
    }

    and: "produce more messages"
    producer.send("c")
    producer.send("d")

    then:
    subscriber.awaitCount(4)
    subscriber.assertValues("a", "b", "c", "d")

    when:
    consumer.close()

    then:
    subscriber.awaitTerminalEvent()
    subscriber.assertComplete()

    cleanup:
    producer.close();
    broker.close()
  }

  def "should eventually connect and get messages"() {
    given:
    def port = ThreadLocalRandom.current().nextInt(1024, 10_000)
    def options = new ActiveMqConsumerOptions().setMaxReconnectDelay(Duration.ofMillis(100))
    def consumer = ActiveMqConsumer.create(vertx, options)
    def subscriber = new TestObserver()
    def connectSobscriber = new TestObserver()

    when:
    consumer.connect("tcp://localhost:$port").subscribe(connectSobscriber)
    consumer
      .listen()
      .ofType(TextMessage)
      .map { it.text }
      .subscribe(subscriber)

    and: "sleep a while to simulate the broker is not running when connected"
    sleep(10_000)

    and: "broker starts later"
    def broker = startBroker(port)
    def producer = createProducer(port)

    then: "await the connected successfully"
    eventually {
      assert consumer.connected
    }
    connectSobscriber.awaitTerminalEvent()
    connectSobscriber.assertComplete()

    when:
    consumer.topic("topic")

    and:
    producer.send("a")
    producer.send("b")

    then:
    subscriber.awaitCount(2)
    subscriber.assertValues("a", "b")

    when:
    consumer.close()

    then:
    subscriber.awaitTerminalEvent()
    subscriber.assertComplete()

    cleanup:
    producer.close();
    broker.close()
  }
}
