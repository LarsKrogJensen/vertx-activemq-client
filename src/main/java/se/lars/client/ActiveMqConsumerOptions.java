package se.lars.client;

import java.time.Duration;

public class ActiveMqConsumerOptions {
  private int maxStartupConnectAttemps = 10;
  private int maxReConnectAttemps = -1;
  private Duration initialReconnectDelay = Duration.ofMillis(100);
  private Duration maxReconnectDelay = Duration.ofSeconds(10);

  public ActiveMqConsumerOptions setMaxStartupConnectAttemps(int maxStartupConnectAttemps) {
    this.maxStartupConnectAttemps = maxStartupConnectAttemps;
    return this;
  }

  public ActiveMqConsumerOptions setMaxReConnectAttemps(int maxReConnectAttemps) {
    this.maxReConnectAttemps = maxReConnectAttemps;
    return this;
  }

  public ActiveMqConsumerOptions setInitialReconnectDelay(Duration initialReconnectDelay) {
    this.initialReconnectDelay = initialReconnectDelay;
    return this;
  }

  public ActiveMqConsumerOptions setMaxReconnectDelay(Duration maxReconnectDelay) {
    this.maxReconnectDelay = maxReconnectDelay;
    return this;
  }

  public String formatUrl(String baseUrl) {
    return "failover:(" + baseUrl + ")" +
      "?initialReconnectDelay=" + initialReconnectDelay.toMillis() +
      "&maxReconnectDelay=" + maxReconnectDelay.toMillis() +
      "&maxReconnectAttempts=" + maxReConnectAttemps +
      "&startupMaxReconnectAttempts=" + maxStartupConnectAttemps;
  }
}
