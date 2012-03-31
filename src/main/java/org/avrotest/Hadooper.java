package org.avrotest;

import java.io.*;
import java.net.URI;
import org.apache.avro.*;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
/*
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.AvroWrapper;
*/
import org.apache.avro.util.Utf8;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.*;
import java.util.Scanner;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.nio.charset.Charset;

public class Hadooper {

	// This mapper doesn't read or write anything from the Hadoop runtime..
	//Takes as input 1) Name of source file, Name of output HDFS File, Name of Schema
	public static class HadooperMap extends Mapper<LongWritable, Text, NullWritable, NullWritable> {
		 
		
		private String inFile;
		private String outFile;
		private String avroFile;
		private String outputPath;
		
		protected void setup(final Context context) 
		{ 
			
			// input file from local disk.
			inFile = context.getConfiguration().get("inFile");
			// name of HDFS output file
			outFile = context.getConfiguration().get("outFile");
			// file containing schema
			avroFile = context.getConfiguration().get("avroFile");
			// HDFS Directory for this job
			outputPath = context.getConfiguration().get("outputPath");
			
		}
		
		public void map(LongWritable key, Text value,Text out, 
				                    Context context) throws IOException {
			
			System.out.println("HELLO WORLD! INSIDE HADOOP MAPPER.");
			
			/*
			try {
				  FileWriter fstream;
				  fstream = new FileWriter("outputfile2.txt");
				  BufferedWriter out1 = new BufferedWriter(fstream);
				  out1.write("this is a test, joker...s");
				  out1.close();
			
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			*/
			
			// Create file in HDFS
			Configuration conf = new Configuration();
		    FileSystem fs = FileSystem.get(conf);
		    fs.mkdirs(new Path(outputPath));
		    OutputStream os = fs.create(new Path(outputPath+"/"+outFile));
		  
		    // Get Schema from avsc file
			InputStream in = getClass().getResourceAsStream("/"+avroFile);			
			@SuppressWarnings("deprecation")
			Schema schema = Schema.parse(in);
		    
			// Create a Writer for HDFS
		    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
		    DataFileWriter<GenericRecord> dataFileWriter =
		      new DataFileWriter<GenericRecord>(datumWriter);
		    dataFileWriter.create(schema, os);
		    	    
		    
		    // open the input file
			Scanner st = new Scanner(inFile);
					  
			int i = 0;
		
			String kmer = "";
			String qvalue = "";
			String id = "";
					  
			//read the input file.
			while(st.hasNext())
			{
				if (i == 0){ 
					id = st.nextLine(); 
					i++;
					continue;
				} 
					  	
				if (i == 1){ 
					kmer = st.nextLine(); 
					i++;
					continue;
				}
					  	
				if (i == 2){
					i++; 
					continue;
				}
							
				else {
					qvalue = st.nextLine();
					i = 0;
					GenericRecord record = new GenericData.Record(schema);
					//Schema specific code.
					record.put("id",id);
					record.put("qvalue", qvalue);
					record.put("kmer", kmer);
					
					System.out.println(record.get(kmer));
					//write record to HDFS
					dataFileWriter.append(record);
				}
						
			}
			dataFileWriter.close();
			os.close();       

		}
	}
	
		
	public static void Run(String outputPath, String inFile, String outFile, String schema) throws Exception {
		    Configuration conf = new Configuration();
		    conf.set("inFile", inFile);
		    conf.set("outFile", outFile);
		    conf.set("outputPath", outputPath);
		    conf.set("schema", schema);
		    
		    Job job = new Job(conf, "Avro Test..");
		    job.setJarByClass(HadooperMap.class);
		    job.setMapperClass(HadooperMap.class);
		    job.setInputFormatClass(FileInputFormat.class);
		    
		    
		    job.setMapOutputKeyClass(LongWritable.class);
		    job.setMapOutputValueClass(Text.class); 
		    //Configuration conf2 = new Configuration();
		    FileSystem fs = FileSystem.get(conf);
		    //Deleting output path if exists
		    Path fp = new Path("testout");
		    if (fs.exists(fp)) {
		    	   // remove the file first
		    	         fs.delete(fp);
		    	       }
		    
		    FileInputFormat.addInputPath(job, new Path("test"));
		    FileOutputFormat.setOutputPath(job, new Path("testout"));
		    //conf.set("fs.default.name", "local");
		    if(job.waitForCompletion(true)==true)
		    {
		    	return;
		    	
		    }
		  }
}
