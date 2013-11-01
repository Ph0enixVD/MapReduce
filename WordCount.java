import java.util.List;


public class WordCount implements MapReduceBase {
  
  private static final long serialVersionUID = 1L;

  @Override
	public void map(String key, String val, Context context) {
		String[] words = val.split(" ");
		for (String s : words) {
			context.write(s, "1");
		}
	}

	@Override
	public void reduce(String key, List<String> vals, Context context) {
		int count = 0;
		for (String s : vals) {
			try {
				count += Integer.parseInt(s.trim());
			} catch (Exception e) {
				throw new RuntimeException("what should we do here?");
			}
		}
		context.write(key, Integer.toString(count));
	}

}
