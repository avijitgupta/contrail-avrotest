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
import org.avrotest.createPairedReadsForFlash.RunFlashMapper;
import org.avrotest.kmerCounter.KmerCounterMapper;
import org.avrotest.kmerCounter.KmerCounterReducer;

import java.util.*;

import java.util.Date;

public class createPairedReadsForQuake {
	final static int MAX= 2;
    public static int cutoff;
    static int KmerSize;

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
	
	public static class RunCorrectOnPairedMapper 
    extends AvroMapper<joinedfqrecord, joinedfqrecord>
  {	

	String filePathFq1,filePathFq2;
	String localTime;
	private long K;
	private ArrayList<String> a1;
	private ArrayList<String> a2;
	private String quakehome;
	private String quakedata;
	private String quake_mates_out;
	private String hadoophome;
	
	int count ;
	@Override
	 public void configure(JobConf job) 
    {
    	Calendar calendar = Calendar.getInstance();
        java.util.Date now = calendar.getTime();
        java.sql.Timestamp currentTimestamp = new java.sql.Timestamp(now.getTime());
        int time = currentTimestamp.getNanos();
        
        quakedata = job.get("quakedata");
        quake_mates_out = job.get("quake_mates_out");
        quakehome = job.get("quakehome");
        hadoophome = job.get("hadoophome");
        
        localTime = ""+time;
        //filePathFq1 = "/home/hduser/workspace/QuakeData/testcorrect_1.fq";
        //filePathFq1 = "/home/hduser/workspace/QuakeData/testcorrect_1.fq";
        
		filePathFq1 = quakedata+"/"+time+"correct_1.fq";
		filePathFq2 = quakedata+"/"+time+"correct_2.fq";
		
		K = job.getLong("K", 0);
		a1= new ArrayList<String>();
	    a2= new ArrayList<String>();
	    count = 0;
		//System.out.println("cutoff in setup"+cutOff+" "+ Cutoff.cutoff);
	}
  
	 public void map(joinedfqrecord joined_record, 
	            AvroCollector<joinedfqrecord> output, Reporter reporter) throws IOException {
    
    
		    String seqId = joined_record.id1.toString();
		    String seq1 = joined_record.read1.toString();
		    String qval1 = joined_record.qvalue1.toString();
		    String seqId2 = joined_record.id2.toString();
		    String seq2 = joined_record.read2.toString();
		    String qval2 = joined_record.qvalue2.toString();
    
  //  Text word = new Text();
    //IntWritable one= new IntWritable(1);
      
    a1.add(seqId+"/1\n"+seq1+"\n"+"+\n"+qval1);
    a2.add(seqId+"/2\n"+seq2+"\n"+"+\n"+qval2);
    count ++;
    
    
    if(count == 10000)
    {

        writeLocalFile(a1,a2,filePathFq1,filePathFq2);  
        a2.clear();
        a1.clear();
        count =0;
    }
    		
    output.collect(joined_record);
   }
    @Override
    public void close() throws IOException
	{
    	if(count > 0)
    	{
    		writeLocalFile(a1,a2,filePathFq1,filePathFq2);
    		count = 0;
    	}
    	runCorrectForPairedReads.correctRunner(filePathFq1, filePathFq2, localTime,K, quakehome, quakedata, quake_mates_out,hadoophome);
    	/*
    	File fp = new File(filePathFq1);
    	if(fp.exists())fp.delete();
    	fp = new File(filePathFq2);
    	if(fp.exists())fp.delete();
    	*/
    }
}
	public static void createPairedReads(String inputPath, String outputPath) throws Exception 
	{
		JobConf conf = new JobConf(createPairedReadsForQuake.class);
	    conf.setJobName("Copying Joined files to Local and Running Quake ");
		
	    ///Basic Initialization
	    
	    conf.setLong("K", ContrailConfig.K);
		conf.set("quakedata",ContrailConfig.Quake_Data);
	    conf.set("quake_mates_out", ContrailConfig.Quake_Final_Out);
	    conf.set("quakehome",ContrailConfig.Quake_Home);
	    conf.set("hadoophome",ContrailConfig.Hadoop_Home);
	    
	    AvroJob.setMapperClass(conf, RunCorrectOnPairedMapper.class);

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
		/*Configuration conf = new Configuration();
		conf.setLong("K", ContrailConfig.K);
		conf.set("quakedata",ContrailConfig.Quake_Data);
	    conf.set("quake_mates_out", ContrailConfig.Quake_Final_Out);
	    conf.set("quakehome",ContrailConfig.Quake_Home);
	    conf.set("hadoophome",ContrailConfig.Hadoop_Home);
	    
	    conf.setInt("mapred.line.input.format.linespermap", 2000000); // must be a multiple of 4
		System.out.println("Inside createPairedReads");
	    Job job = new Job(conf, "Paired Reads Creation");    
	    job.setJarByClass(createPairedReadsForQuake.class);
	    job.setMapperClass(CorrectMapper.class);
	    //job.setReducerClass(KmerReducer.class);
	    job.setOutputValueClass(IntWritable.class);
	    job.setOutputKeyClass(Text.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class); 
	    
	    Configuration conf2 = new Configuration();
	    FileSystem fs = FileSystem.get(conf2);
	    //Deleting output path if exists
	    Path fp = new Path(outputPath);
	    Path fp1 = new Path(ContrailConfig.Quake_Final_Out);
	    
	    if (fs.exists(fp1))
	    {
	    	fs.delete(fp1);	
	    }
	    
	    fs.mkdirs(fp1);
	    
	    
	    if (fs.exists(fp)) {
	    	   // remove the file first
	    	         fs.delete(fp);
	    	       }
	    
	    FileInputFormat.addInputPath(job, new Path(inputPath));
	    FileOutputFormat.setOutputPath(job, new Path(outputPath));
	    if(job.waitForCompletion(true)==true)
	    {
	    	return;
	    	
	    }*/
	  }
}
