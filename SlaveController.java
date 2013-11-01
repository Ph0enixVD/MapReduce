
import java.io.BufferedReader;
import java.io.InputStreamReader;

public class SlaveController {
  public static void main(String args[]) {
    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
    try {
      int port = Integer.parseInt(reader.readLine());

      SlaveTaskTracker taskTracker = new SlaveTaskTracker(port);
      taskTracker.start();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
