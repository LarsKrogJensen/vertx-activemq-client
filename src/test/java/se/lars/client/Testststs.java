package se.lars.client;

import java.util.regex.Pattern;

public class Testststs {
  public static void main(String[] args) {
    String pattern = "failover:\\((\\S*)\\)(?:\\?(\\S*))?";
    Pattern p = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);

    doMatch(p, "failover:(tcp://localhost:616?connectTimeout=30)?timeout=3000&abc=123&ryrry=9812");
    doMatch(p, "failover:(tcp://localhost:616?connectTimeout=30)?timeout=3000&abc=123");
    doMatch(p, "failover:(tcp://localhost:616?connectTimeout=30)?timeout=3000");
    doMatch(p, "failover:(tcp://localhost:616?connectTimeout=30)");
  }

  public static void doMatch(Pattern p, String text) {

    System.out.println("TEXT: " + text);
    var matcher = p.matcher(text);
    boolean matches = matcher.matches();
    System.out.println("Matches: " + matches + ", Groups: " + matcher.groupCount());
    if (matches) {
      for (int i = 0; i <= matcher.groupCount(); i++) {
        System.out.println("group #" + i + ": " + matcher.group(i));
      }
    }
    System.out.println("-------------------------");
  }


}
