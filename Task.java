import java.io.Serializable;


public interface Task extends Serializable{
	public int getSlaveId();
	public void setSlaveId(int slaveId);
	public int getTaskId();
	public void setTaskId(int taskId);
	public int getJobId();
	public void setJobId(int jobId);
	public MapReduceJob getJob();
	public void setJob(MapReduceJob job);
}
