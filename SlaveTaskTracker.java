import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

public class SlaveTaskTracker {

  String masterAddr;
  int port;
  String slaveAddr;
  List<String> slaveList;
  Map<Integer, Integer> reduceInputRequestProgress;

  public SlaveTaskTracker(int port) {
    try {
      this.port = port;
      String masterIp = InetAddress.getLocalHost().getHostAddress();
      this.slaveAddr = InetAddress.getLocalHost().getHostAddress() + ":" + port;
      masterAddr = masterIp + ":12345";

      slaveList = new ArrayList<String>();
      slaveList.add(InetAddress.getLocalHost().getHostAddress() + ":15619");
      slaveList.add(InetAddress.getLocalHost().getHostAddress() + ":15719");
      
      reduceInputRequestProgress = new HashMap<Integer, Integer>();

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
        ObjectOutputStream out = new ObjectOutputStream(
            socket.getOutputStream());
        out.writeObject(response);
        out.flush();
        out.close();
        socket.close();

        if (type == MessageType.MsgMapTaskStart) {
          MapTask maptask = (MapTask) msg.getObj();
          System.out.println("get MsgMapTaskStart");
          mapTaskStart(maptask);
        }
        if (type == MessageType.MsgShuffleSortStart) {
          System.out.println("get MsgShuffleSortStart");
          int jid = (Integer) msg.getObj();
          int num_reducer = (Integer) msg.getArg();
          shuffleAndSort(jid, num_reducer);
        }
        if (type == MessageType.MsgReduceTaskStart) {
          System.out.println("get MsgReduceTaskStart");
          ReduceTask reducetask = (ReduceTask) msg.getObj();
          reduceTaskStart(reducetask);
        }
        if (type == MessageType.MsgReduceInputRequest) {
          System.out.println("get MsgReduceInputRequest");
          String slave_src = (String) msg.getArg();
          ReduceTask task = (ReduceTask) msg.getObj();
          handleReduceInputRequest(task, slave_src);
        }
        if (type == MessageType.MsgReduceInputResponse) {
          System.out.println("get MsgReduceInputResponse");
          String slave_src = (String) msg.getSecondArg();
          ReduceTask task = (ReduceTask) msg.getArg();
          String input = (String) msg.getObj();
          handleReduceInputResponse(input, task, slave_src);
        }

      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  public void shuffleAndSort(final int jid, int num_reducer) {
    
    System.out.println("shuffle sort start jid="+jid);
    try {
      File directory = new File("src/tmp/");
      String[] myFiles = directory.list(new FilenameFilter() {
        public boolean accept(File directory, String fileName) {
          return fileName.startsWith("map_out_" + jid + "_" + slaveAddr);
        }
      });

      List<PrintWriter> wlist = new ArrayList<PrintWriter>();
      for (int i = 0; i < num_reducer; i++) {
        PrintWriter pw = new PrintWriter(new FileWriter("src/tmp/sort_out_"
            + jid + "_" + i + "_" + slaveAddr, true));
        wlist.add(i, pw);
      }

      for (String filename : myFiles) {
        System.out.println("read file "+filename);
        BufferedReader br = new BufferedReader(new FileReader("src/tmp/"
            + filename));
        String line;
        while ((line = br.readLine()) != null) {
          String key = line.split(" ")[0];
          int n = Math.abs(key.hashCode()) % num_reducer;
          wlist.get(n).println(line);
        }
        br.close();
      }

      for (PrintWriter pw : wlist) {
        pw.close();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

    System.out.println("shuffle sort finish");
    Message msg = new Message(MessageType.MsgShuffleSortFinish, jid, null);
    sendMsgToMaster(msg);
  }

  public void mapTaskStart(MapTask m) {
    mapRunner mapper = new mapRunner(m);
    mapper.start();
  }

  public class mapRunner extends Thread {

    MapTask task;

    public mapRunner(MapTask task) {
      this.task = task;
    }

    @Override
    public void run() {
      try {
        System.out.println("here we start map task!");
        int jid = task.getJobId();
        int taskid = task.getTaskId();
        int offset = task.getOffset();
        MapReduceJob job = task.getJob();
        String inputsplit = task.getInputSplit();
        Scanner scanner = new Scanner(inputsplit);
        PrintWriter pw = new PrintWriter(new FileWriter("src/tmp/map_out_"
            + jid + "_" + slaveAddr + "_" + taskid, true));
        Context context = new Context();
        context.setOutputWriter(pw);
        while (scanner.hasNextLine()) {
          String line = scanner.nextLine();
          job.getMapperReducer().map(Integer.toString(offset++), line, context);
        }

        pw.flush();
        pw.close();
      } catch (Exception e) {
        e.printStackTrace();
      }

      System.out.println("map task finished");
      Message msg = new Message(MessageType.MsgMapTaskFinish, task, null);
      sendMsgToMaster(msg);
    }

  }

  private void sendMsgToMaster(Message m) {
    MsgDispatcher dispatcher = new MsgDispatcher(m, masterAddr);
    dispatcher.start();
  }

  private void sendMsgToSlave(Message m, String slave) {
    MsgDispatcher dispatcher = new MsgDispatcher(m, slave);
    dispatcher.start();
  }

  public class MsgDispatcher extends Thread {
    private Message m;
    private String dstAddr;

    public MsgDispatcher(Message m, String dstAddr) {
      this.m = m;
      this.dstAddr = dstAddr;
    }

    public void run() {
      Socket s;
      try {
        String ip = dstAddr.split(":")[0];
        String port = dstAddr.split(":")[1];
        s = new Socket(ip, Integer.parseInt(port));
        ObjectOutputStream os = new ObjectOutputStream(s.getOutputStream());
        os.writeObject(m);
        os.flush();
        ObjectInputStream is = new ObjectInputStream(s.getInputStream());
        Message responseMsg = (Message) is.readObject();
        if (responseMsg.getType() != MessageType.MsgOK)
          throw new RuntimeException("MSG ERROR");
        s.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  public void requestReduceInput(ReduceTask task) {
    Message msg = new Message(MessageType.MsgReduceInputRequest, task,
        slaveAddr);
    for (String slave : slaveList) {
      if (!slave.equals(slaveAddr)) {
        sendMsgToSlave(msg, slave);
      }
    }
  }

  public void handleReduceInputRequest(ReduceTask task, String src_slave) {
    try {
      int jid = task.getJobId();
      int tid = task.getTaskId();

      String filename = "src/tmp/sort_out_" + jid + "_" + tid + "_"
          + slaveAddr;
      BufferedReader br = new BufferedReader(new FileReader(filename));
      String line;
      StringBuilder builder = new StringBuilder();
      while ((line = br.readLine()) != null) {
        builder.append(line);
        builder.append("\n");
      }
      br.close();

      Message msg = new Message(MessageType.MsgReduceInputResponse,
          builder.toString(), task);
      msg.setSecondArg(slaveAddr);
      sendMsgToSlave(msg, src_slave);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void handleReduceInputResponse(String input, ReduceTask task,
      String inputFromAddr) {
    try {
      int jid = task.getJobId();
      int tid = task.getTaskId();
      PrintWriter pw = new PrintWriter(new FileWriter("src/tmp/reduce_in_"
          + jid + "_" + tid + "_" + inputFromAddr, true));
      pw.write(input);
      pw.flush();
      pw.close();
      int n = reduceInputRequestProgress.get(jid) - 1;
      reduceInputRequestProgress.put(jid, n);
      if (n == 0) {
        reduceTaskContinue(task);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void reduceTaskStart(ReduceTask reducetask) {
    if (slaveList.size() == 1) {
      reduceTaskContinue(reducetask);
    } else {
      int jid = reducetask.getJobId();
      reduceInputRequestProgress.put(jid, slaveList.size() - 1);
      requestReduceInput(reducetask);
    }
  }

  public void reduceTaskContinue(ReduceTask reducetask) {
    try {
      final int jid = reducetask.getJobId();
      final int tid = reducetask.getTaskId();
      MapReduceJob job = reducetask.getJob();
      File directory = new File("src/tmp/");
      String[] myFiles = directory.list(new FilenameFilter() {
        public boolean accept(File directory, String fileName) {
          return fileName.startsWith("reduce_in_" + jid + "_" + tid) || fileName.startsWith("sort_out_"+ jid + "_" + tid+"_"+ slaveAddr);
        }
      });

      HashMap<String, List<String>> reduceMap = new HashMap<String, List<String>>();

      for (String filename : myFiles) {
        System.out.println("read reduce input from "+filename);
        BufferedReader br = new BufferedReader(new FileReader("src/tmp/"
            + filename));
        String line;
        while ((line = br.readLine()) != null) {
          String[] kv = line.split(" ");
          String k = kv[0];
          String v = kv[1];
          if (reduceMap.containsKey(k)) {
            reduceMap.get(k).add(v);
          } else {
            List<String> list = new LinkedList<String>();
            list.add(v);
            reduceMap.put(k, list);
          }
        }
        br.close();
      }
      
      System.out.println("Reducer"+tid+" start to write output to "+job.getOutputFile()+"_"+jid+"_"+tid);

      
      PrintWriter pw = new PrintWriter(new FileWriter(
          job.getOutputFile()+"_"+jid+"_"+tid, true));
      Context context = new Context();
      context.setOutputWriter(pw);
      for (String k : reduceMap.keySet()) {
        List<String> v = reduceMap.get(k);
        job.getMapperReducer().reduce(k, v, context);
      }
      pw.flush();
      pw.close();
      

      System.out.println("reduce task "+tid+" finished");
      Message msg = new Message(MessageType.MsgReduceTaskFinish, reducetask, null);
      sendMsgToMaster(msg);

      // TODO: clean resources
    } catch (Exception e) {
      e.printStackTrace();
    }

  }
}
