package org.avrotest;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


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
	
public static class KmerMapper 
       extends Mapper<Object, Text, Text, IntWritable>
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
    private ArrayList<String> a;
    int count = 0;
    int flagStarting;
    protected void setup(final Context context) 
	{
    	Calendar calendar = Calendar.getInstance();
        java.util.Date now = calendar.getTime();
        java.sql.Timestamp currentTimestamp = new java.sql.Timestamp(now.getTime());
        localTime = ""+currentTimestamp.getNanos();
		
		quakehome = context.getConfiguration().get("quakehome");
		quakedata = context.getConfiguration().get("quakedata");
		hadoophome = context.getConfiguration().get("hadoophome");
		singles_out = context.getConfiguration().get("singles_out");
		
		K = context.getConfiguration().getLong("K", 0);
		a = new ArrayList<String>();
		filePath = quakedata+"/"+localTime+"_quakesingle.fq";
		//flagStarting = 0;
		//System.out.println("cutoff in setup"+cutOff+" "+ Cutoff.cutoff);
	}
    
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    
   
    String line = value.toString();
    idx++;
   /* if(flagStarting==0 && line.charAt(0)!='@')
    {
    	return;
    }
    else
    {
    	
    	flagStarting =1;
    	 
    }*/
    if(idx == 1){line1 = line;}
    if(idx == 2){line2 = line;}
    if(idx == 3){line3 = line;}
    if(idx == 4)
    {
    	line4 = line;
    	a.add(line1+"\n"+line2+"\n"+line3+"\n"+line4);
    	count ++;
    	if(count==10000)
    	{
    		CorrectSinglesInvocationStub.writeLocalFile(a,filePath);
    		a.clear();
    		count =0;
    		Text word = new Text();
            IntWritable one= new IntWritable(1);
            word.set(line);
            context.write(word, one);
    	}

    	// I have reservations if this will be split on line boundary - mapred.linespermap is old api - D
    	idx = idx%4;
    	
    }
 
  }
	
	@Override
	protected void cleanup(final Context context)
	{
		if(count > 0)
		{
			CorrectSinglesInvocationStub.writeLocalFile(a,filePath);
			a.clear();
    		count =0;
		}
		CorrectLocal.runcode(filePath,localTime,K,hadoophome,quakehome,quakedata,singles_out);
		
		//deleting Local File
		//File fp = new File(filePath);
		//if(fp.exists())fp.delete();
	}
}

  

  @SuppressWarnings("deprecation")
public static void run(String inputpath, String outputpath) throws Exception {
	  
   
	Configuration conf = new Configuration();
	conf.setLong("K", ContrailConfig.K);
	conf.set("quakehome", ContrailConfig.Quake_Home);
	conf.set("quakedata",ContrailConfig.Quake_Data);
	conf.set("hadoophome", ContrailConfig.Hadoop_Home);
	conf.set("singles_out",ContrailConfig.Quake_Singles_Out);
	
	
	//conf.setInt("mapreduce.input.lineinputformat.linespermap", 2000000); // must be a multiple of 4
	conf.setInt("mapred.task.timeout", 0);
	    
    Job job = new Job(conf, "Correct Singles Invocation Stub");
    
    NLineInputFormat.setNumLinesPerSplit(job, 2000000);


    Calendar calendar = Calendar.getInstance();
    java.util.Date now = calendar.getTime();
    java.sql.Timestamp currentTimestamp = new java.sql.Timestamp(now.getTime());
    
    job.setJarByClass(CorrectSinglesInvocationStub.class);
    job.setMapperClass(KmerMapper.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    Configuration conf2 = new Configuration();
    FileSystem fs = FileSystem.get(conf2);
    Path fp = new Path(outputpath);
    Path singles_out = new Path(ContrailConfig.Quake_Singles_Out);
    if (fs.exists(singles_out))
    {
    	fs.delete(singles_out);
    	
    }
    // Create the Output Directory in HDFS. - Data is written to this from within Mapper.
    fs.mkdirs(singles_out);
    if (fs.exists(fp)) {
    	   // remove the file first
    	         fs.delete(fp);
    	       }
    
    FileInputFormat.setInputPaths(job, inputpath);
    //addInputPath(job, new Path(inputpath));
    // this output path is junk.
   
    FileOutputFormat.setOutputPath(job, new Path(outputpath));
    
    if(job.waitForCompletion(true)==true)
    {
    	return;
    	
    }
    }
	
}
