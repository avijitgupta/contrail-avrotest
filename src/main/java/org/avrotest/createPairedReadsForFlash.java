package org.avrotest;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapred.Pair;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.util.Utf8;
import org.apache.avro.Schema;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.avrotest.kmerCounter.KmerCounterMapper;
import org.avrotest.kmerCounter.KmerCounterReducer;

import java.util.*;

import java.util.Date;

public class createPairedReadsForFlash 
{

	static void writeLocalFile(ArrayList<String> a1,ArrayList<String> a2,String fp1, String fp2)
	{
		try{
				//First File
			  FileWriter fstream = new FileWriter(fp1,true);
			  BufferedWriter out = new BufferedWriter(fstream);
			  for(int i=0;i<a1.size();i++)
			  {
				  out.write(a1.get(i)+"\n");
			  }
			  out.close();
			  fstream.close();
			
			  	//Second File
			  fstream = new FileWriter(fp2,true);
			  out = new BufferedWriter(fstream);
			  for(int i=0;i<a2.size();i++)
			  {
				  out.write(a2.get(i)+"\n");
			  }
			  out.close();
			  fstream.close();
		
		
		}
			  catch (Exception e)	{e.printStackTrace();}
	}
	
	public static class RunFlashMapper 
    extends AvroMapper<joinedfqrecord, joinedfqrecord>
       //extends Mapper<Object, Text, Tex>
  {	
	
	String filePathFq1,filePathFq2;
	String localTime;
	String FlashFinalOut=null;
	String FlashLocalOut = null;
	String FlashHome = null;
	String HadoopHome = null;
	String CorrectInPath = null;
	private ArrayList<String> temp_arraylist_1;
	private ArrayList<String> temp_arraylist_2;
	int count;
	@Override
	 public void configure(JobConf job) 
    {
    	Calendar calendar = Calendar.getInstance();
        java.util.Date now = calendar.getTime();
        java.sql.Timestamp currentTimestamp = new java.sql.Timestamp(now.getTime());
        int time = currentTimestamp.getNanos();
        localTime = ""+time;
        String Flash_Node_In = job.get("flashLocalInput");
        filePathFq1 = Flash_Node_In+"/"+time+"_1.fq";
		filePathFq2 = Flash_Node_In+"/"+time+"_2.fq";
		FlashFinalOut = job.get("flashHDFSOutput");
		FlashLocalOut = job.get("flashLocalOutput");
		FlashHome = job.get("flashHome");
		HadoopHome = job.get("HadoopHome");
		CorrectInPath = job.get("CorrectInPath");
		temp_arraylist_1= new ArrayList<String>();
		temp_arraylist_2= new ArrayList<String>();
	    count = 0;
		//filePathFq1 = "/home/hduser/fq1.fq";
        //filePathFq2 = "/home/hduser/fq2.fq";
		//System.out.println("cutoff in setup"+cutOff+" "+ Cutoff.cutoff);
	}
  
	 public void map(joinedfqrecord joined_record, 
	            AvroCollector<joinedfqrecord> output, Reporter reporter) throws IOException {
	    
	    
	   // int j;
	   // StringTokenizer i = new StringTokenizer(value.toString());
	    String seqId = joined_record.id1.toString();
	    String seq1 = joined_record.read1.toString();
	    String qval1 = joined_record.qvalue1.toString();
	    String seqId2 = joined_record.id2.toString();
	    String seq2 = joined_record.read2.toString();
	    String qval2 = joined_record.qvalue2.toString();
	    
	    //Text word = new Text();
	    //IntWritable one= new IntWritable(1);
	  /*  ArrayList<String> a1= new ArrayList<String>();
	    ArrayList<String> a2= new ArrayList<String>();*/
	    //cutoff = 12; //value not propogating
	  //  System.out.println(seqId+"/1\n"+seq1+"\n"+"+\n"+qval1);
	    //System.out.println(seqId+"/2\n"+seq2+"\n"+"+\n"+qval2);
	    count ++;
	    temp_arraylist_1.add(seqId+"/1\n"+seq1+"\n"+"+\n"+qval1);
	    temp_arraylist_2.add(seqId+"/2\n"+seq2+"\n"+"+\n"+qval2);
	    if(count ==10000)
	    {
		    
		    //word.set(seqId);
		    //context.write(word, one);
		    createPairedReadsForFlash.writeLocalFile(temp_arraylist_1,temp_arraylist_2,filePathFq1,filePathFq2);  
		    temp_arraylist_1.clear();
		    temp_arraylist_2.clear();
		    count =0;
	    }	
	   
	    output.collect(joined_record);
	    
	   }
	 	
	 	public void close() throws IOException
		{
	    	if(count > 0)
	    	{
	    		createPairedReadsForFlash.writeLocalFile(temp_arraylist_1,temp_arraylist_2,filePathFq1,filePathFq2);
	    		count = 0;
	    	}
	    	runFlash.flashRunner(filePathFq1, filePathFq2, localTime,FlashFinalOut, FlashLocalOut, FlashHome, HadoopHome,CorrectInPath);
	    	//Cleaning Up local Files
	    	/*
	    	File fp = new File(filePathFq1);
	    	if(fp.exists())fp.delete();
	    	fp = new File(filePathFq2);
	    	if(fp.exists())fp.delete();
	    	*/
	    }
}
	public static void run(String inputPath, String outputPath) throws Exception 
	{
		
		JobConf conf = new JobConf(createPairedReadsForFlash.class);
	    conf.setJobName("Copying Joined files to Local and Running Flash ");
		
	    ///Basic Initialization
	    
	    conf.set("flashLocalInput", ContrailConfig.Flash_Node_In);
		conf.set("flashHDFSOutput", ContrailConfig.Flash_Final_Out);
		conf.set("flashHome", ContrailConfig.Flash_Home);
		conf.set("flashLocalOutput", ContrailConfig.Flash_Node_Out);
		conf.set("HadoopHome", ContrailConfig.Hadoop_Home);
		conf.set("CorrectInPath", ContrailConfig.CorrectInDirectory);
	    
	    AvroJob.setMapperClass(conf, RunFlashMapper.class);

	    FileInputFormat.addInputPath(conf, new Path(inputPath));
	    FileOutputFormat.setOutputPath(conf, new Path(outputPath));
	    ///Input is the kmer count file in Avro. So we used Kmer count recrod schema
	    joinedfqrecord read = new joinedfqrecord();
	    AvroJob.setInputSchema(conf, read.getSchema());
	   ////We dont require an output from this
	    AvroJob.setMapOutputSchema(conf, read.getSchema());
	    
	   // AvroJob.setReducerClass(conf, KmerCounterReducer.class);
	       
	    //Map Only Job
	    conf.setNumReduceTasks(0);
	 // Delete the output directory if it exists already
	    Path out_path = new Path(outputPath);
	    if (FileSystem.get(conf).exists(out_path)) {
	      FileSystem.get(conf).delete(out_path, true);  
	    }
	    
	    long starttime = System.currentTimeMillis();            
	    JobClient.runJob(conf);
	    long endtime = System.currentTimeMillis();
	    float diff = (float) (((float) (endtime - starttime)) / 1000.0);
	    System.out.println("Runtime: " + diff + " s");
	    return ;
		
	  }
	}
	

