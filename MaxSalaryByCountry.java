import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxSalaryByCountry {
  
  public static class Map extends Mapper<LongWritable,Text,Text,Text>    
  {  
   public void map(LongWritable k,Text v, Context con)throws IOException, InterruptedException  
   {  
    String line = v.toString();  
    String[] w=line.split(",");  
    int sal=Integer.parseInt(w[3]);  
    String name=Integer.parseInt(w[1]);
    con.write(new Text(name), new Text(name+","+sal));  
    }  
  } 
 
  public static class Reduce extends Reducer<Text,Text,IntWritable,Text>  
  {  
   public void reduce(Text k, Iterable<Text> vlist, Context con)
   throws IOException , InterruptedException  
      {  
       int max=0;  
       for(Text v:vlist)  
    {
         String line = v.toString();  
         String[] w=line.split(",");  
         int sal=Integer.parseInt(w[1]); 
         max=Math.max(max, sal);
    }  
    con.write(new IntWritable(max), k);  
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
