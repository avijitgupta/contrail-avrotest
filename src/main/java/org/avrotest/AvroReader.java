package org.avrotest;

import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.Pair;

import java.io.IOException;
import java.io.File;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
//import org.apache.hadoop.mapreduce.FileInputFormat;

//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapred.Reporter;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.io.DatumReader;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.util.Utf8;
import org.apache.avro.file.DataFileReader;

/** Tests mapred API with a specific record. */

public class AvroReader {
/*
	//public static final Schema schema; 
	public static final Schema IN_SCHEMA = ReflectData.get().getSchema(FqRecord.class);
	 public static final Schema MAP_OUT_SCHEMA = 
	            Pair.getPairSchema(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.STRING));
	
//to do - manually delete outputpath
  public void AvroReaderRunner(String inputPath, String outputPath) throws Exception {
    
	  
	JobConf conf = new JobConf(AvroReader.class);
    conf.setJobName("conversion" + inputPath);


    //AvroJob.setMapOutputSchema(conf, new Pair<Utf8,Utf8>(new Utf8(""), 0L).getSchema() );
    //AvroJob.setSchema(con)


	//Schema stringSchema = Schema.create(Schema.Type.STRING);
    //Schema pairSchema = Pair.getPairSchema(stringSchema, stringSchema);

    

    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));
    
    
    AvroJob.setInputSchema(conf, IN_SCHEMA);
	//AvroJob.setOutputSchema(conf, pairSchema);
	AvroJob.setMapOutputSchema(conf, MAP_OUT_SCHEMA);
	AvroJob.setMapperClass(conf, AvroReaderMapper.class);
    
	System.out.println("AvroReaderRunner");
  
	//////////////
	Job job = new Job(conf,"AvroRecord Job..");
    job.setJobName("avroreader");	
    job.setJarByClass(AvroReader.class);
	/////////////////
    


    //FileInputFormat.addInputPath(job, new Path(inputPath));
    
    //FileOutputFormat.setOutputPath(job, new Path(outputPath));
	conf.setNumReduceTasks(0);
	
	conf.setJarByClass(AvroReader.class);
	
    //job.setNumReduceTasks(0);                     // map-only

    //job.waitForCompletion(true);

	JobClient.runJob(conf);

    }

  // maps input Weather to Pair<Weather,Void>, to sort by Weather
  public static class AvroReaderMapper extends AvroMapper<FqRecord, Pair<String,String>>{
	
	  
	@Override
    public void map(FqRecord in, AvroCollector<Pair<String,String>> output,
                      Reporter reporter) throws IOException {
      
    	System.out.println("Inside AvroReader"+in.id);
    	
    	 File f;
    	  f=new File("/home/avijit/testfile.txt");
    	  if(!f.exists()){
    	  f.createNewFile();
    	  }
    	
    	Pair<String, String> p = new Pair<String,String>(MAP_OUT_SCHEMA);
    	
    	p.set(in.read, in.read);
    	output.collect(p);
    
    }
	
  }
*/
}