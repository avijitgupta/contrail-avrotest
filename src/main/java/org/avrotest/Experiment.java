package org.avrotest;
import java.io.*;
import java.util.Scanner;

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




public class Experiment {

	
	
	public void testwriteavro() throws Exception{
	
		
		String inFile = "/home/deepak/SourceBed/ContrailPlus/data/sample.fq";
		InputStream in = getClass().getResourceAsStream("/fastq.avsc");	
		
	//	String schemastring = "{ \"type\":\"record\"," + " \"name\":\"Fastq\"," + " \"doc\":\"Fastq record\"," + " \"fields\": " +" [{ \"name\":\"id\",\"type\":\"string\"}," + " { \"name\":\"kmer\", \"type\":\"string\"}" + " ]" +" }";
	    

	     File f = new File("/home/deepak/SourceBed/ContrailPlus/data/sample0.fq");

		Schema schema = null ;
		try
		{
		schema = Schema.parse(in);
		}
		catch(Exception e)
		{
			System.out.println(e.toString());
		}
		
		
		GenericRecord datum = new GenericData.Record(schema);
		
		// Create a Writer for HDFS
		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
		DataFileWriter<GenericRecord> dataFileWriter = 	new DataFileWriter<GenericRecord>(datumWriter);
		
	    try {
	    	dataFileWriter.create(schema, f);
	    } catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}	
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
	}
		
		/*
		System.out.println("Hello World");
		
		try{
		InputStream in = getClass().getResourceAsStream("/pair.avsc");
			
		Schema schema = Schema.parse(in);
		System.out.println(schema.getName());
		
		GenericRecord datum = new GenericData.Record(schema);
		int a = 10;
		datum.put("id",a);
		datum.put("kmer", a+10);
		//datum.put("qvalue"),);
	
		File f = new File("/home/deepak/output.txt");
		DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
		DataFileWriter<GenericRecord> filewriter = new DataFileWriter<GenericRecord>(writer);
		 
		    try {
		    	filewriter.create(schema, f);
		    	filewriter.append(datum);
		        filewriter.close();
		    } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		    DatumReader<GenericRecord> reader= new GenericDatumReader<GenericRecord>();
			try {
				DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(f,reader);
				GenericRecord result=dataFileReader.next();
		        System.out.println("data:\t" + result.get("fromv").toString() + result.get("tov").toString());	        
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			    
		}
		
		catch (IOException ex){
			System.out.println(ex.toString());	
		}
	}

	*/
}
