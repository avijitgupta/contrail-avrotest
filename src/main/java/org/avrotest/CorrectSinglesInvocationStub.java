package org.avrotest;

import java.io.BufferedWriter;
import java.io.File;
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

/*
 * This class runs correct on singles
 * Mapper - Writes the input avro format fastq file into normal format on local disk
 * Close() invokes the correct program
 */
public class CorrectSinglesInvocationStub {

	final static int MAX= 2;
    static int cutoff;
    static int KmerSize;
    

	static void writeLocalFile(ArrayList<String> a, String filePath)
	{
		try{
			  FileWriter fstream = new FileWriter(filePath,true);
			  BufferedWriter out = new BufferedWriter(fstream);
			  for(int i=0;i<a.size();i++)
			  {
				  out.write(a.get(i)+"\n");
			  }
			  out.close();
			  fstream.close();
			}
			  catch (Exception e)	{e.printStackTrace();}
	}
	
	public static class RunCorrectOnSinglesMapper 
    extends AvroMapper<fastqrecord, NullWritable>
  {	
	private int idx = 0;
	private String line1 = null;
	private String line2  = null;
	private String line3 = null;
	private String line4  = null;
    private String filePath=null;
    private String localTime = null;
    private String hadoophome = null;
    private String quakehome = null;
    private String quakedata = null;
    private String singles_out = null;
    private long K = 0;
    private ArrayList<String> temp_arraylist;
    int count = 0;
    int flagStarting;
    @Override
    public void configure(JobConf job) 
    {
    	Calendar calendar = Calendar.getInstance();
        java.util.Date now = calendar.getTime();
        java.sql.Timestamp currentTimestamp = new java.sql.Timestamp(now.getTime());
        localTime = ""+currentTimestamp.getNanos();
		
		quakehome = job.get("quakehome");
		quakedata = job.get("quakedata");
		hadoophome = job.get("hadoophome");
		singles_out = job.get("singles_out");
		
		K = job.getLong("K", 0);
		temp_arraylist = new ArrayList<String>();
		filePath = quakedata+"/"+localTime+"_quakesingle.fq";

	}
    
    public void map(fastqrecord fq_record, 
            AvroCollector<NullWritable> output, Reporter reporter) throws IOException {
    
   
    
    	line1 = fq_record.id.toString();
    	line2 = fq_record.read.toString();
    	line3 = "+";
    	line4 = fq_record.qvalue.toString();;
    	temp_arraylist.add(line1+"\n"+line2+"\n"+line3+"\n"+line4);
    	count ++;
    	if(count==10000)
    	{
    		CorrectSinglesInvocationStub.writeLocalFile(temp_arraylist,filePath);
    		temp_arraylist.clear();
    		count =0;
    		
    	}

        output.collect(NullWritable.get());

    	
    }
 
  	@Override
  	 public void close() throws IOException
 	{
		if(count > 0)
		{
			CorrectSinglesInvocationStub.writeLocalFile(temp_arraylist,filePath);
			temp_arraylist.clear();
    		count =0;
		}
		CorrectLocal.runcode(filePath,localTime,K,hadoophome,quakehome,quakedata,singles_out);
		
		//deleting Local File
		File fp = new File(filePath);
		if(fp.exists())fp.delete();
	}
}

  

  @SuppressWarnings("deprecation")
public static void run(String inputPath, String outputPath) throws Exception {
	  
   
	JobConf conf = new JobConf(createPairedReadsForQuake.class);
	// Basic Initialisation
	conf.setJobName("Running Correct Local ");
	conf.setLong("K", ContrailConfig.K);
	conf.set("quakehome", ContrailConfig.Quake_Home);
	conf.set("quakedata",ContrailConfig.Quake_Data);
	conf.set("hadoophome", ContrailConfig.Hadoop_Home);
	conf.set("singles_out",ContrailConfig.Quake_Singles_Out);
	
	conf.setInt("mapred.task.timeout", 0);
    AvroJob.setMapperClass(conf, RunCorrectOnSinglesMapper.class);
    FileInputFormat.addInputPaths(conf, inputPath);
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));
    fastqrecord read = new fastqrecord();
    AvroJob.setInputSchema(conf, read.getSchema());
    conf.setNumReduceTasks(0);
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
