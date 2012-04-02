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
import org.avrotest.kmerCounter.KmerCounterMapper;
import org.avrotest.kmerCounter.KmerCounterReducer;

import java.util.*;

import java.util.Date;

/*
 * The input to this class is the counts file. 
 * This counts file is emitted by mappers on their local fs
 * The local fs contains quake which we have modified to give out 
 * numeric bithases. These numeric bithashes are generated locally and later
 * are copied into the common directory on which mapreduce task is run
 * to get the global numeric bithash. This still needs to be converted into
 * quakecompatiblebithash.
 */

public class localBitHash {

    public static String Input;
    static FileWriter fstream;
    static BufferedWriter out;

        
	static void writeLocalFile(ArrayList<String> a)
	{
		try{
			  		
			//  
			  for(int i=0;i<a.size();i++)
			  {
				  out.write(a.get(i)+"\n");
			  }
			  
			}
			  catch (Exception e)	{e.printStackTrace();}
	}
	
    
public static class BitHashMapper 
       extends AvroMapper<Pair<Utf8, Long>, NullWritable>
  {	
	private  ArrayList<String> temp_arraylist;
	private String datapath;
	private int count;
	private String localtime;
	private String filepath;
    private long cutoff;
    private long K;
    private String bitout;
    private String hadoophome;
    private String quakehome;
	
    public void configure(JobConf job) 
    {
    	Calendar calendar = Calendar.getInstance();
        java.util.Date now = calendar.getTime();
        java.sql.Timestamp currentTimestamp = new java.sql.Timestamp(now.getTime());
        int time = currentTimestamp.getNanos();
        localtime = ""+time;
        cutoff = job.getLong("cutoff", 0);
		datapath = job.get("Input");
		bitout = job.get("BitOutput");
		filepath = datapath+"/"+localtime+"_localbithash";
		hadoophome = job.get("hadoophome");
		K = job.getLong("K", 0);
		quakehome = job.get("quakehome");
		temp_arraylist = new ArrayList<String>();
		
		
		try{
		
		String q1= hadoophome+"/bin/hadoop dfs -mkdir "+bitout+"/";
		Process p = Runtime.getRuntime().exec(q1);
		}
		catch(Exception e){
			
			System.out.print("Caught exception "+e.toString());
		}
		
		
		
		try
		{
			fstream = new FileWriter(filepath,true);
			out = new BufferedWriter(fstream);
		
		}
		catch(Exception e)
		{

			System.out.print("Caught exception "+e.toString()+ " " + fstream);
		}
    	
    }
    
   
	
	//incoming key,value pairs - (kmer, frequency)
    public void map(Pair<Utf8, Long> count_record, 
            AvroCollector<NullWritable> output, Reporter reporter) throws IOException {
    
	    String kmer;
	    String frequency;
	    kmer= count_record.key().toString();
	    frequency = count_record.value().toString();
	 
	    temp_arraylist.add(kmer+"\t"+frequency);
	    count++;
	    		
	    if(count>=1000)
	    {
	    	
	    	localBitHash.writeLocalFile(temp_arraylist);  
	    	temp_arraylist.clear();
	    	count = 0;
	    }
    	output.collect(NullWritable.get());
    }
    
    
    public void close() throws IOException
	{
    	if(count>0)
    	{
    		localBitHash.writeLocalFile(temp_arraylist);  
    	}
    	
    	try{
    	  out.close();
		  fstream.close();
    	}catch(Exception e){}
    
    	try{
    		
    		String k = Long.toString(K);
    		String q = quakehome+"/src/build_bithash -k "+k+" -c "+cutoff+" -m "+filepath+" -o "+filepath+"_out"; 
		
    		
		System.out.println(q);
     	Process p = Runtime.getRuntime().exec(q);
     	try {
			p.waitFor();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
     	
     
		
		 Configuration conf2 = new Configuration();
		    FileSystem fs = FileSystem.get(conf2);
		    //Copying the local Bithash file into a common folder in HDFS
		    Path fp1 = new Path(filepath+"_out");
		    Path fp2 = new Path(bitout);
		    fs.copyFromLocalFile(fp1, fp2);
		    ///Cleaning up local files
		    File fp = new File(filepath);
			if(fp.exists())fp.delete();
			
			fp = new File(filepath+"_out");
			if(fp.exists())fp.delete();
    	}
		 catch (IOException e) {e.printStackTrace(); }
    
    }
  }
    	
  @SuppressWarnings("deprecation")
public static void bitHashlocal(String inputPath, String outputPath, long K, long cutoff) throws Exception {

	  	JobConf conf = new JobConf(kmerCounter.class);
	    conf.setJobName("Generating Local Bithashes ");
		///Basic Initialization
	    
	    conf.set("Input", ContrailConfig.Quake_Data);
		conf.set("BitOutput", ContrailConfig.BitHash_Local_Temp); 
		conf.setLong("K", K);
		conf.setLong("cutoff", cutoff);
		conf.set("hadoophome",ContrailConfig.Hadoop_Home);
	    conf.set("quakehome",ContrailConfig.Quake_Home);
	    
	    FileInputFormat.addInputPath(conf, new Path(inputPath));
	    FileOutputFormat.setOutputPath(conf, new Path(outputPath));
	    ///Input is the kmer count file in Avro. So we used Kmer count recrod schema
	    AvroJob.setInputSchema(conf, new Pair<Utf8,Long>(new Utf8(""), 0L).getSchema());
	   ////We dont require an output from this
	 //   AvroJob.setOutputSchema(conf, new Pair<Utf8,Long>(new Utf8(""), 0L).getSchema());
	    AvroJob.setMapperClass(conf, BitHashMapper.class);
	   // AvroJob.setReducerClass(conf, KmerCounterReducer.class);
	       
	    //Map Only Job
	    conf.setNumReduceTasks(0);
	 // Delete the output directory if it exists already
	    Path out_path = new Path(outputPath);
	    if (FileSystem.get(conf).exists(out_path)) {
	      FileSystem.get(conf).delete(out_path, true);  
	    }
	    //The Bit Hash Temporary directory on HDFS should not exist initially
	    Path bit_hash_temp_out = new Path(ContrailConfig.BitHash_Local_Temp);
	    if (FileSystem.get(conf).exists(bit_hash_temp_out)) {
		      FileSystem.get(conf).delete(bit_hash_temp_out, true);  
		    }
	    
	    //Create the bithash temporary directory
	    FileSystem.get(conf).mkdirs(bit_hash_temp_out);
	   
	    long starttime = System.currentTimeMillis();            
	    JobClient.runJob(conf);
	    long endtime = System.currentTimeMillis();
	    float diff = (float) (((float) (endtime - starttime)) / 1000.0);
	    System.out.println("Runtime: " + diff + " s");
	    return ;
	
	  
    }
  }