import java.io.Serializable;
public class ReduceTask implements Serializable, Task {
	
	private static final long serialVersionUID = -235332325L;
	
	private int slaveId;
	private int taskId;
	private int jobId;
	private MapReduceJob job;
	
	public ReduceTask(int taskId, int jobId, MapReduceJob j) {
		this.taskId = taskId;
		this.jobId = jobId;
		this.job = j;
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
