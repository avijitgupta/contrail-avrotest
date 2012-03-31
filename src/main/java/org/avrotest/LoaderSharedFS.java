package org.avrotest;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Scanner;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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

public class LoaderSharedFS implements ILoaderSource {

	
	public void copyDataToLocal(String src, String dstLocalDir) {
		
		String filename = new File(src).getName();
		copy(src,dstLocalDir+"/"+filename);
		
	}

	public void loadDataToHdfs(String srcPath, String dstHdfsDir, String schemaFile) {
		
		
		System.out.println("Inside loaderDatatoHdfs");
		System.out.println("schemaFile:\t"+schemaFile);
		System.out.println("dstHdfsDir:\t"+dstHdfsDir);
		System.out.println("srcPath:\t"+srcPath);
		
		// Create file in HDFS
		String filename = new File(srcPath).getName();
		Configuration conf = new Configuration();
		
		try{
		
			FileSystem fs = FileSystem.get(conf);
		
			OutputStream os = fs.create(new Path(dstHdfsDir+"/"+filename));
				  
			// Get Schema from avsc file
			
			InputStream in = getClass().getResourceAsStream("/"+schemaFile);			
			
			Schema schema = Schema.parse(in);
			//System.out.println(schema.getName())
			
			
			
			// Create a Writer for HDFS
			DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
			DataFileWriter<GenericRecord> dataFileWriter =
			new DataFileWriter<GenericRecord>(datumWriter);
			dataFileWriter.create(schema, os);
				
			// open the input file
			Scanner st = new Scanner(new File(srcPath));
							  
			int i = 0;
				
			String kmer = "";
			String qvalue = "";
			String id = "";
		

			GenericRecord record = new GenericData.Record(schema);
			
			int count = 0;
			//read the input file.
			while(st.hasNext())
			{
				//System.out.println("Inside while loop");
				//System.out.println(st.nextLine());
				//System.out.println(st.nextLine());
				count++;
				if (i == 0){ 
					id = st.nextLine(); 
					
					//System.out.println(schema.getDoc()+ schema.getName());
					
					record.put("id",id);
					/*
					if (count == 1) 
					{
						
						//System.out.println(id);
						//System.out.println("Record ID:"+ record.get("id"));
					}
					*/
					
					i++;
					continue;
				} 
							  	
				if (i == 1){ 
					kmer = st.nextLine(); 
					//System.out.println(kmer);
					record.put("kmer", kmer);
					//System.out.println(record.get(kmer));
					i++;
					continue;
				}
							  	
				if (i == 2){
					i++; 
					continue;
				}
									
				else {
					qvalue = st.nextLine();
					record.put("qvalue", qvalue);
					
					i = 0;
					
					//Schema specific code.
					
					//System.out.println("Record Value:"+record.get(kmer));
					//write record to HDFS
					dataFileWriter.append(record);
					//System.out.println(kmer);
					
				}
								
			}
			
			dataFileWriter.close();
			os.close();       
		
		
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
		
	//utility function to copy a file from one place to anothee
	//we use this to copy file from remote to a local path.
	//the local file is then transformed to avro. 
	public void copy(String inputFile, String outputFile)
	{
		
	    int c;
	    
	    //System.out.println(inputFile +"\t"+outputFile);
	    
	    try {
	    	FileReader in = new FileReader(inputFile);
		    FileWriter out = new FileWriter(outputFile);
		    
	    	while ((c = in.read()) != -1)
	    	{
	    		//System.out.println(c);
	    		out.write(c);
	    	}
	    		
		    in.close();
		    out.close();
	    } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
