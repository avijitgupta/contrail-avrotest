package org.avrotest;

import java.io.File;
import java.io.IOException;
import java.util.List;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.avrotest.Hadooper.HadooperMap;
import org.apache.avro.mapred.*;

//takes file as input, and creates as many map tasks as the number of lines in the file.
public class SpawnMapsTest {


	public static class SpawnMap extends Mapper<LongWritable, Text, NullWritable, NullWritable> {
		
		// output DFS Directory
		private String dfsDir;
		// local temp Directory for copying data.
		private String localDir;
		
		public void setup(final Context context)
		{
			localDir = context.getConfiguration().get("localDir",null);
			dfsDir = context.getConfiguration().get("dfsDir", null); 
		}
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String srcFilePath = value.toString();		
			System.out.println(srcFilePath +"\t"+localDir+"\t"+dfsDir);
			//change this to get data from WebServer.
			ILoaderSource loader = new LoaderSharedFS();
			
			//copy data from srcFilePath to localDir
			//To Do (dnettem) - extract filename from srcFilePath, and replace one.txt with it.
			loader.copyDataToLocal(srcFilePath, localDir);
			
			//loader.copyDataToLocal(srcFilePath,"/home/hduser/tempout/one.txt");
			
			//get only file name from the path
			String filename = new File(srcFilePath).getName();
			//load to HDFS in specified dfsDir with same name.
			loader.loadDataToHdfs(localDir+"/"+filename,dfsDir,"fastq.avsc");
		}
	}	
	
		@SuppressWarnings("deprecation")
		public static void SpawnRunner(String inputPath, String outputPath) throws Exception {
		    Configuration conf = new Configuration();
		    
		    
		    //localDir is the path to directory on local machine 
		    //where the temp files copied from remote location will be saved
		    // before loading to Avro.
		    String localDir = "/home/deepak/SourceBed/avrotest/AvroTest/target/tempout";
		    conf.set("localDir",localDir);
		    //final output directory on HDFS.
		    conf.set("dfsDir",outputPath);
		    conf.set("outputPath", outputPath);
		    
		    Job job = new Job(conf, "Spawn Test..");
		    job.setJarByClass(SpawnMap.class);
		    job.setMapperClass(SpawnMap.class);
		    job.setInputFormatClass(OneLineInputFormat.class);
		    job.setMapOutputKeyClass(LongWritable.class);
		    job.setMapOutputValueClass(Text.class); 
		    
		    FileSystem fs = FileSystem.get(conf);
		    
		    Configuration conf1 = new Configuration();
		    //Deleting output path if exists

		   fs.delete(new Path("spawnout"));
		   fs.delete(new Path(outputPath));
		    
		    
		    //create the output directory
		    fs.mkdirs(new Path(outputPath));
		    
		    FileInputFormat.addInputPath(job, new Path(inputPath));
		    FileOutputFormat.setOutputPath(job, new Path("spawnout"));
		    		    
		    if(job.waitForCompletion(true)==true)
		    {
		    	return;
		    	
		    }
		  }	
}