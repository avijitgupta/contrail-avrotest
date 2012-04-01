package org.avrotest;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class CreateGlobalBitHash {

	public static class BitHashMapper 
    extends Mapper<Object, Text, LongWritable, LongWritable>{
	
 public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
 
	String line = value.toString();
	StringTokenizer st = new StringTokenizer(line);
	long offset = Long.parseLong(st.nextToken());
	long bithash = Long.parseLong(st.nextToken());
	context.write(new LongWritable(offset),new LongWritable(bithash));	
 
}

}
public static class BitHashReducer extends Reducer<LongWritable,LongWritable,LongWritable,LongWritable> {
    
	 
 public void reduce(LongWritable key, Iterable<LongWritable> values, 
                    Context context
                    ) throws IOException, InterruptedException {
     
     long count = 0;
  	for (LongWritable val : values) {
  		long bithash = Long.parseLong(val.toString());
  		count = count | bithash;
   }
 	 context.write(key,new LongWritable(count));
   }
}

@SuppressWarnings("deprecation")
public static void BitMapGenerator(String inputPath, String outputPath) throws Exception {
 Configuration conf = new Configuration();
 Job job = new Job(conf, "Generating Global bithash");
 job.setJarByClass(CreateGlobalBitHash.class);
 job.setMapperClass(BitHashMapper.class);
 job.setReducerClass(BitHashReducer.class);
 job.setOutputKeyClass(LongWritable.class);
 job.setOutputValueClass(LongWritable.class);
 job.setMapOutputKeyClass(LongWritable.class);
 job.setMapOutputValueClass(LongWritable.class); 
 
 Configuration conf2 = new Configuration();
 FileSystem fs = FileSystem.get(conf2);
 
 Path fp = new Path(outputPath);
 if (fs.exists(fp)) {
 	   // remove the file first
 	         fs.delete(fp);
 	       }
 
 FileInputFormat.addInputPath(job, new Path(inputPath));
 FileOutputFormat.setOutputPath(job, new Path(outputPath));
 if(job.waitForCompletion(true)==true)
 {
 	return;
 	
 }
}
}
