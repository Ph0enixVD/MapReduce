import java.io.PrintWriter;


public class Context {
  private PrintWriter pw;
  public void setOutputWriter(PrintWriter pw) {
    this.pw = pw;
  }
  public void write(String key, String val) {
    pw.println(key+" "+val);
  }
}
