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
 * This class prepares quake run
 * Mapper -Writes local file after splitting up the joined record that it gets as input
 * Output files are written in blocks of 1000.
 * In the close(), correct is run on local files
 */
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
    extends AvroMapper<joinedfqrecord, NullWritable>
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
        
        /*Temporary Filenames get their name from the timestamp*/        
		filePathFq1 = quakedata+"/"+time+"correct_1.fq";
		filePathFq2 = quakedata+"/"+time+"correct_2.fq";
		
		K = job.getLong("K", 0);
		a1= new ArrayList<String>();
	    a2= new ArrayList<String>();
	    count = 0;
	}
  
	 public void map(joinedfqrecord joined_record, 
	            AvroCollector<NullWritable> output, Reporter reporter) throws IOException {
    
    
		    String seqId = joined_record.id1.toString();
		    String seq1 = joined_record.read1.toString();
		    String qval1 = joined_record.qvalue1.toString();
		    String seqId2 = joined_record.id2.toString();
		    String seq2 = joined_record.read2.toString();
		    String qval2 = joined_record.qvalue2.toString();
    
      
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
    		
    output.collect(NullWritable.get());
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
    	
    	//Deleting Local Temporary files created by the mapper
    	
    	File fp = new File(filePathFq1);
    	if(fp.exists())fp.delete();
    	fp = new File(filePathFq2);
    	if(fp.exists())fp.delete();
    	
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

	    /* Input is a joined schema which containes two mate files joined*/
	    joinedfqrecord read = new joinedfqrecord();
	    AvroJob.setInputSchema(conf, read.getSchema());
	   	       
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
