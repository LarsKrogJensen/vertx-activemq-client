package se.lars.client;

import java.time.Duration;

public class ActiveMqConsumerOptions {
  private int maxStartupConnectAttemps = 10;
  private int maxReConnectAttemps = -1;
  private Duration initialReconnectDelay = Duration.ofMillis(100);
  private Duration maxReconnectDelay = Duration.ofSeconds(10);

  /**
   * Controls how many connections attempts before failure, default 10
   * the consumer will retry until succeeds, and give an error message after this number of attempts
   */
  public ActiveMqConsumerOptions setMaxStartupConnectAttemps(int maxStartupConnectAttemps) {
    this.maxStartupConnectAttemps = maxStartupConnectAttemps;
    return this;
  }

  /**
   * Max reconnect attempts. Is is excluding the startup reconnect, default is -1 which mean reconnect forever and ever
   */
  public ActiveMqConsumerOptions setMaxReConnectAttemps(int maxReConnectAttemps) {
    this.maxReConnectAttemps = maxReConnectAttemps;
    return this;
  }

  /**
   * Initial backof delay before attempting to reconnect, default 100 ms
   */
  public ActiveMqConsumerOptions setInitialReconnectDelay(Duration initialReconnectDelay) {
    this.initialReconnectDelay = initialReconnectDelay;
    return this;
  }

  /**
   * Max reconnect delay, during reconnect the delay will increase with each retry but not longer that maxReconnectDelay
   */
  public ActiveMqConsumerOptions setMaxReconnectDelay(Duration maxReconnectDelay) {
    this.maxReconnectDelay = maxReconnectDelay;
    return this;
  }

  /**
   * Formats the failover broker url
   */
  public String formatUrl(String baseUrl) {
    return "failover:(" + baseUrl + ")" +
      "?initialReconnectDelay=" + initialReconnectDelay.toMillis() +
      "&maxReconnectDelay=" + maxReconnectDelay.toMillis() +
      "&maxReconnectAttempts=" + maxReConnectAttemps +
      "&startupMaxReconnectAttempts=" + maxStartupConnectAttemps;
  }
}
