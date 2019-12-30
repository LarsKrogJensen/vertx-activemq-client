package se.lars;

import static java.lang.System.setProperty;

public class Main {

  public static void main(String[] args) {
    setProperty(
      "vertx.logger-delegate-factory-class-name",
      "io.vertx.core.logging.SLF4JLogDelegateFactory"
    );


    var app = new Application();
    app.start();
  }
}
