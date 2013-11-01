import java.io.Serializable;
public class MapReduceJob implements Serializable {
  
  private static final long serialVersionUID = -11223134L;
  private String inputFile;
  private String outputFile;
  private MapReduceBase mrBase;
  
  public void setInputFile(String inputFile) {
    this.inputFile = inputFile;
  }

  public void setOutputFile(String outputFile) {
    this.outputFile = outputFile;
  }

  public String getInputFile() {
    return inputFile;
  }

  public String getOutputFile() {
    return outputFile;
  }
	public MapReduceBase getMapperReducer() {
	  return this.mrBase;
	}
	
	public void setMapReduceBase(MapReduceBase mrBase) {
	  this.mrBase= mrBase;
	}

}
