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


public class testmax {

public static class empmap extends Mapper<Object, Text, Text, Text> {

public void map(Object key,Text value,Context ctx) throws IOException, InterruptedException
{
String[] arr=value.toString().split("\\s");
// String[] arr=value.toString().split(",");
ctx.write(new Text(arr[2].toString()), new Text((arr[1].toString()) + " " +arr[4].toString()));

}
}


public static class empreduce extends Reducer<Text, Text,Text, Text>
{
public void reduce(Text key,Iterable<Text> itr,Context context) throws IOException, InterruptedException
{ int maxsal=0;
String s= "" ;
String sal = " ";

for (Text val : itr){
String arr[] = val.toString().split("\\s");
// Sytem.out.println(val);
// String[] arr = val.toString().split(",");
if (maxsal < Integer.parseInt(arr[1]))
{
maxsal = Integer.parseInt(arr[1]);
sal = arr[1].toString();

s = arr[0].toString();

}

}
// System.out.println(key + "\t" + (s.toString() +"  " + sal.toString()));
context.write(new Text(key), new Text(s.toString() +"  " + sal.toString()));

}

}




public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException
{
Configuration conf=new Configuration();
Job job=Job.getInstance(conf,"emain");
job.setJarByClass(testmax.class);
job.setMapperClass(empmap.class);
//job.setNumReduceTasks(0);
job.setReducerClass(empreduce.class);
//job.setMapOutputKeyClass(Text.class);
////job.setReducerClass(Empreduce.class);
//job.setMapOutputValueClass(Text.class);
job.setOutputKeyClass(Text.class);
//job.setOutputValueClass(ArrayWritable.class);
job.setOutputValueClass(Text.class);
FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
System.exit(job.waitForCompletion(true) ? 0 : 1);

}

}


//-----------------------
//stackoverflow

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
   int sal=Integer.parseInt(w[2]);  
  //  string name=Integer.parseInt(w[1]);
  //  con.write(new Text(name), new Text(name+","+sal));  
  // 
  // String line = v.toString();  
  // String[] w=line.split(","); 
  String name = w[1] ; 
  // int sal=Integer.parseInt(w[2]);  
  String map_op = name+","+sal ; 
  con.write(new Text("ds"), new Text(map_op));
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

        // int salary = Integer.parseInt(v.toString().split(",")[1]) ;
        //  max=Math.max(max, salary); 
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


//-------

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
    job.setOutputValueClass(IntWritable.class);
    job.setMapOutputKeyClass(Text.class);
job.setMapOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
