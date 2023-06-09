//Nama : Okta Gilang Al Jaffarsyah
//Kelas : 3SI2

import java.io.IOException;
import javax.print.event.PrintEvent;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxSalary {

  public static class Map extends Mapper<Object, Text, Text, Text> {

    public void map(Object key, Text value, Context ctx) throws IOException, InterruptedException {
      String[] arr = value.toString().split(",");
      ctx.write(new Text(arr[1].toString()), new Text((arr[0].toString()) + "," + arr[2].toString()));
    }
  }

  public static class Reduce extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> itr, Context context) throws IOException, InterruptedException {
      int maxsal = 0;
      String s = "";
      String sal = " ";

      for (Text val : itr) {
        String arr[] = val.toString().split(",");
        if (maxsal < Integer.parseInt(arr[1])) {
          maxsal = Integer.parseInt(arr[1]);
          sal = arr[1].toString();
          s = arr[0].toString();
        }
      }
      context.write(new Text(key), new Text(s.toString() + "  " + sal.toString()));
    }
  }

  public static void main(String[] args)
      throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "MaxSalary");
    job.setJarByClass(MaxSalary.class);
    job.setMapperClass(Map.class);
    // job.setNumReduceTasks(0);
    job.setReducerClass(Reduce.class);
    // job.setMapOutputKeyClass(Text.class);
    //// job.setReducerClass(Empreduce.class);
    // job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    // job.setOutputValueClass(ArrayWritable.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }

}