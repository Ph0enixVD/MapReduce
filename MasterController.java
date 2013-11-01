
import java.io.BufferedReader;
import java.io.InputStreamReader;

public class MasterController {

  public static void main(String args[]) {
    MasterJobTracker jobTracker = new MasterJobTracker();
    jobTracker.start();

    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

    while (true) {

      // read input
      try {
        String input = reader.readLine();
        String[] jobArgs = input.split(" ");

        if (jobArgs[0].equals("start")) {
          if (jobArgs.length < 4) {
            System.out.println("Format: start (jobclass) (inputfile) (outputfile)");
            continue;
          }
          
          
          String jobName = jobArgs[1];

          MapReduceBase mrBase = (MapReduceBase) Class.forName(jobName).newInstance();

          MapReduceJob job = new MapReduceJob();
          
          job.setMapReduceBase(mrBase);
          
          job.setOutputFile(jobArgs[3]);

          job.setInputFile(jobArgs[2]);

          jobTracker.newMapJob(job);

          System.out.println(jobName + " started!");
          
          continue;
        }
      } catch (Exception e) {
        e.printStackTrace();
      }

    }

  }
}
