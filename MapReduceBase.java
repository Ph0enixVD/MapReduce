import java.io.Serializable;
import java.util.List;

public interface MapReduceBase extends Serializable{
  public void map(String key, String val,Context context);
  public void reduce(String key, List<String> vals, Context context);
}
