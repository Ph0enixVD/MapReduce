import java.io.BufferedReader;
import java.io.FileReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;


class MasterJobTracker {

  private int nextjobid = 0;

  private Map<Integer, MapReduceJob> idToJob = new HashMap<Integer, MapReduceJob>();
  // k = jid v = progress
  private Map<Integer, Integer> jobProgress = new HashMap<Integer, Integer>();

  private Map<Integer, String> slaveAddr = new HashMap<Integer, String>();

  // slave load  k=slave id  v = load
  private Map<Integer, Integer> slaveMapLoad = new HashMap<Integer, Integer>();
  private Map<Integer, Integer> slaveReduceLoad = new HashMap<Integer, Integer>();

  // key=job id value= remaining tasks of the job
  private TreeMap<Integer, List<Task>> mapQueue = new TreeMap<Integer, List<Task>>();
  private TreeMap<Integer, List<Task>> reduceQueue = new TreeMap<Integer, List<Task>>();

  public MasterJobTracker() {

    try {
      String localhost = InetAddress.getLocalHost().getHostAddress();
      slaveAddr.put(0, localhost + ":15619");
      slaveAddr.put(1, localhost + ":15719");
      slaveMapLoad.put(0, 0);
      slaveMapLoad.put(1, 0);
      slaveReduceLoad.put(0, 0);
      slaveReduceLoad.put(1, 0);
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

  public void start() {
    ListenerThread listener = new ListenerThread();
    listener.start();
  }
  
  public class ListenerThread extends Thread {
 
    public ListenerThread() {
    }
    
    public void run() {
      ServerSocket serverSock = null;
      int port = 12345;
      try {
        serverSock = new ServerSocket(port);
        while (true) {
          Socket socket = serverSock.accept();
          MsgHandler handler = new MsgHandler(socket);
          handler.start();
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  public void newMapJob(MapReduceJob j) {

    int jid = nextjobid++;

    int tid = 0;

    List<Task> tasks = new LinkedList<Task>();

    int num_per_map = 20;

    String filename = j.getInputFile();

    StringBuilder builder = new StringBuilder();

    try {
      BufferedReader br = new BufferedReader(new FileReader(filename));
      int count = 0;
      String line;
      while ((line = br.readLine()) != null) {
        count++;
        builder.append(line);
        builder.append("\n");
        if (count % num_per_map == 0) {
          String split = builder.toString();
          builder = new StringBuilder();
          MapTask m = new MapTask(tid++, jid, split, j);
          m.setOffset(count-num_per_map);
          System.out.println("[input split]offset is " + (count-num_per_map));
          tasks.add(m);
        }
      }
      System.out.print("count=" + count);
      String split = builder.toString();
      if (!split.equals("")) {
        MapTask m = new MapTask(tid++, jid, split, j);
        m.setOffset(count-count%num_per_map);
        System.out.println("[input split]here offset is " + (count-count%num_per_map));
        tasks.add(m);
      }
      br.close();

    } catch (Exception e) {
      e.printStackTrace();
    }

    idToJob.put(jid, j);
    mapQueue.put(jid, tasks);
    jobProgress.put(jid, tasks.size());

    System.out.println("add a new job, job id = : " + jid + "  total task= "+ tid);

    distributeMapTasks(jid, tasks);
  }

  public void distributeMapTasks(int jobid, List<Task> tasks) {

    int map_per_slave = 5;

    // calculate locality here. now use a simple version

    while (tasks.size() > 0) {
      int freeSlave = -1;
      for (int slave : slaveMapLoad.keySet()) {
        if (slaveMapLoad.get(slave) != map_per_slave)
          freeSlave = slave;
      }
      if (freeSlave == -1) {
        // all slaves are busy
        break;
      } else {
        Task task = tasks.remove(0);
        task.setSlaveId(freeSlave);
        slaveMapLoad.put(freeSlave, slaveMapLoad.get(freeSlave) + 1);
        Message msg = new Message(MessageType.MsgMapTaskStart, task, null);
        System.out.println("send task" + task.getTaskId() + " to slave "
            + freeSlave);
        SendMsgToSlave(msg, freeSlave);
      }
    }

    if (tasks.size() == 0) {
      mapQueue.remove(jobid);
    }
  }

  private void SendMsgToSlave(Message m, int slaveId) {
    MsgDispatcher dispatcher = new MsgDispatcher(m, slaveId);
    dispatcher.start();
  }

  public class MsgDispatcher extends Thread {

    private Message m;
    private int slaveId;

    public MsgDispatcher(Message m, int slaveId) {
      this.m = m;
      this.slaveId = slaveId;
    }

    public void run() {
      Socket s;
      try {
        String ip = slaveAddr.get(slaveId).split(":")[0];
        String port = slaveAddr.get(slaveId).split(":")[1];
        s = new Socket(ip, Integer.parseInt(port));

        ObjectOutputStream os = new ObjectOutputStream(s.getOutputStream());
        System.out.println("msg sent to" + ip+" "+port);
        os.writeObject(m);
        ObjectInputStream is = new ObjectInputStream(s.getInputStream());
        Message responseMsg = (Message) is.readObject();
        if (responseMsg.getType() != MessageType.MsgOK)
          throw new RuntimeException("MSG ERROR");
        System.out.println("msg sent to slave "+ip+" "+port+" success");

        s.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  public class MsgHandler extends Thread {

    Socket socket;
    public MsgHandler(Socket socket) {
      this.socket = socket;
    }

    @Override
    public void run() {
      processMessage(socket);
    }
    
    public void processMessage(Socket socket) {
      ObjectInputStream is;
      try {
        is = new ObjectInputStream(socket.getInputStream());
        Message msg = (Message) is.readObject();
        MessageType type = msg.getType();
        Message response = new Message(MessageType.MsgOK, null, null);
        ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
        out.writeObject(response);
        out.flush();
        out.close();
        socket.close();
        if (type == MessageType.MsgMapTaskFinish) {
          System.out.println("Get MsgMapTaskFinish");
          Task task = (Task) msg.getObj();
          mapTaskFinished(task);
        }
        if (type == MessageType.MsgReduceTaskFinish) {
          System.out.println("Get MsgReduceTaskFinish");
          Task task = (Task) msg.getObj();
          reduceTaskFinished(task);
        }
        if(type == MessageType.MsgShuffleSortFinish) {
          System.out.println("Get MsgShuffleSortFinish");
          int jid = (Integer)msg.getObj();
          shuffleSortFinished(jid);
        }

      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
  
  public void shuffleSortFinished(int jid){
    int n = jobProgress.get(jid)-1;
    System.out.println("shuffle sort progress="+n);
    jobProgress.put(jid, n);
    if(n==0){
      System.out.println("shuffle sort finish. start reduce");
      //shuffle sort finished , start reduce 
      newReduceJob(jid);
    }
  }
    

  public void mapTaskFinished(Task t) {
    System.out.println("Map Task finished jid ==" + t.getJobId() + " tid=="
        + t.getTaskId());

    int jid = t.getJobId();
    int slaveid = t.getSlaveId();

    if (mapQueue.size() != 0) {
      int nextjb = mapQueue.firstKey();
      List<Task> nextTaskList = mapQueue.get(nextjb);
      Task nextTask = nextTaskList.remove(0);
      if (nextTaskList.size() == 0) {
        mapQueue.remove(nextjb);
      }
      nextTask.setSlaveId(slaveid);
      Message msg = new Message(MessageType.MsgMapTaskStart, nextTask, null);
      System.out.println("send task" + nextTask.getTaskId() + " to slave "
          + slaveid);
      SendMsgToSlave(msg, slaveid);

    } else {
      slaveMapLoad.put(slaveid, slaveMapLoad.get(slaveid) - 1);
    }

    int progress = jobProgress.get(jid) - 1;
    jobProgress.put(jid,progress);
    if (progress > 0) {
      System.out.println("map progress = "+progress);
      // what to do here?
    } else {
      // map phase finished, start shuffle and sort
      System.out.println("map progress = "+progress);
      System.out.println("shuffle sort start");
      // reset job progress to number of slaves
      jobProgress.put(jid, slaveAddr.size());
      shuffleAndSortStart(jid);
    }
  }

  private void shuffleAndSortStart(int jid) {
    int num_reducer = 2;
    Message msg = new Message(MessageType.MsgShuffleSortStart, jid, num_reducer);
    for(int i=0;i<slaveAddr.size(); i++) {
       SendMsgToSlave(msg, i);
    }
  }

  private void newReduceJob(int jid) {
    int num_reducer = 2;
    List<Task> tasks = new LinkedList<Task>();
    for(int i=0;i<num_reducer;i++) {
      MapReduceJob j = idToJob.get(jid);
      Task rtask = new ReduceTask(i,jid,j);
      tasks.add(rtask);
    }
    reduceQueue.put(jid, tasks);
    System.out.println("[reduce]reduce start progress = "+ num_reducer);
    jobProgress.put(jid, num_reducer);
    distributeReduceTasks(jid,tasks);
  }

  public void distributeReduceTasks(int jobid, List<Task> tasks) {
    int reduce_per_slave = 1;

    while (tasks.size() > 0) {
      int freeSlave = -1;
      for (int slave : slaveReduceLoad.keySet()) {
        int n;
        if ( (n=slaveReduceLoad.get(slave)) != reduce_per_slave) {
          slaveReduceLoad.put(slave, n+1);
          freeSlave = slave;
          break;
        }
      }
      if (freeSlave == -1) {
        System.out.println("[reduce]all slaves are busy, lets wait");
        break;
      } else {
        System.out.println("[reduce]free slave is slave"+freeSlave);
        Task task = tasks.remove(0);
        task.setSlaveId(freeSlave);
        Message msg = new Message(MessageType.MsgReduceTaskStart, task, null);
        System.out.println("send reduce task" + task.getTaskId() + " to slave "+ freeSlave);
        SendMsgToSlave(msg, freeSlave);
      }
    }

    if (tasks.size() == 0) {
      reduceQueue.remove(jobid);
    }
  }

  
  public void reduceTaskFinished(Task t) {
    System.out.println("Reduce Task finished jid ==" + t.getJobId() + " tid=="
        + t.getTaskId());

    int jid = t.getJobId();
    int slaveid = t.getSlaveId();

    if (reduceQueue.size() != 0) {
      int nextjb = reduceQueue.firstKey();
      List<Task> nextTaskList = reduceQueue.get(nextjb);
      Task nextTask = nextTaskList.remove(0);
      if (nextTaskList.size() == 0) {
        reduceQueue.remove(nextjb);
      }
      nextTask.setSlaveId(slaveid);
      Message msg = new Message(MessageType.MsgReduceTaskStart, nextTask, null);
      System.out.println("send task" + nextTask.getTaskId() + " to slave "
          + slaveid);
      SendMsgToSlave(msg, slaveid);
    } else {
      slaveReduceLoad.put(slaveid, slaveReduceLoad.get(slaveid) - 1);
    }

    int progress = jobProgress.get(jid) - 1;
    jobProgress.put(jid,progress);
    if (progress > 0) {
      System.out.println("reduce progress = "+ progress);
    } else {
      System.out.println("reduce progress = "+ progress);
      System.out.println("we finish job "+jid);
      // TODO: clean resources here!
    }
  }

}
