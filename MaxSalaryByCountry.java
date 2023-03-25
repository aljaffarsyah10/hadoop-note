import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxSalaryByCountry {
  
  public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private Text country = new Text();
    private DoubleWritable salary = new DoubleWritable();
    
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line, ",");
      
      String countryName = tokenizer.nextToken();
      String salaryStr = tokenizer.nextToken();
      double salaryValue = Double.parseDouble(salaryStr);
      
      country.set(countryName);
      salary.set(salaryValue);
      
      context.write(country, salary);
    }
  }
  
  public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
      double maxSalary = Double.MIN_VALUE;
      
      for (DoubleWritable value : values) {
        maxSalary = Math.max(maxSalary, value.get());
      }
      
      context.write(key, new DoubleWritable(maxSalary));
    }
  }
  
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "MaxSalaryByCountry");
    job.setJarByClass(MaxSalaryByCountry.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
