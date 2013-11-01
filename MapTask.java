import java.io.Serializable;

public class MapTask implements Serializable, Task{

	private static final long serialVersionUID = -12345678L;
	
	private int taskId;
	private int jobId;
	private int slaveId;
	private MapReduceJob job;
	private String inputSplit;
	private int offset;
	
	
	public MapTask(int _taskId, int _jobId, String split, MapReduceJob j) {
		this.taskId = _taskId;
		this.jobId = _jobId;
		this.inputSplit = split;
		this.job = j;
	}

	 public void setOffset(int offset) {
	    this.offset = offset;
	  }
	  
	  public int getOffset() {
	    return offset;
	  }
	  
	
	public void setInputSplit(String split) {
	  inputSplit = split;
	}
	
	public String getInputSplit() {
	  return inputSplit;
	}
	
	public int getTaskId() {
		return taskId;
	}
	public void setTaskId(int taskId) {
		this.taskId = taskId;
	}
	public int getJobId() {
		return jobId;
	}
	public void setJobId(int jobId) {
		this.jobId = jobId;
	}
	public MapReduceJob getJob() {
		return job;
	}
	public void setJob(MapReduceJob job) {
		this.job = job;
	}

	public int getSlaveId() {
		return slaveId;
	}

	public void setSlaveId(int slaveId) {
		this.slaveId = slaveId;
	}
	
}
