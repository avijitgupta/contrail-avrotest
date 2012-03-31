package org.avrotest;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Hello world!
 *
 */
public class App 
{
		
    public static void main( String[] args )
    {
       try {
    	  
    	//Experiment e = new Experiment();
    	
    	//e.testwriteavro();
		
    	//SpawnMapsTest.SpawnRunner("paths","outputtest");
    	//AvroReader avr = new AvroReader();
    	//avr.AvroReaderRunner("outputtest", "reader_out");
    	//Hadooper.Run(outputPath, inFile, outFile, schema);
       
    	   //TestWordCount rec = new TestWordCount();
    	
    	///Kmer counting Run
    	boolean kmercount =false;
    	boolean avroToNonAvroPartFile = false;
    	boolean calculateCutOff = true;
    	if(kmercount)
    	{
    		kmerCounter k = new kmerCounter();
    		k.run("outputfq", "kmer_count", 13);
    	}
    	   ///Count File Creation from Avro
    	
    	if(avroToNonAvroPartFile)
    	{
    		convertPartFileToNonAvro conv = new convertPartFileToNonAvro();
    		conv.run("kmer_count", "count_file");
    	}  
    	
    	
    	
    	//rec.run("inputfq","outputfq");
    	
       } catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
		
    }
}
