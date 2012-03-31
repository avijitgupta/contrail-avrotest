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
    	ContrailConfig.readXMLConfig();
    	boolean kmercount =false;
    	boolean avroToNonAvroPartFile = false;
    	boolean calculateCutOff = false;
    	boolean pruneCountFile = false;
    	
    	if(kmercount)
    	{
    		kmerCounter k = new kmerCounter();
    		k.run("outputfq", "ContrailPlus/quake_kmer_count", 13);
    	}
    	   ///Count File Creation from Avro
    	
    	if(avroToNonAvroPartFile)
    	{
    		convertPartFileToNonAvro conv = new convertPartFileToNonAvro();
    		conv.run("ContrailPlus/quake_kmer_count", "ContrailPlus/non_avro_count_part");
    	}  
    	
    	if(calculateCutOff)
    	{
    		ContrailConfig.cutoff = cutOffCalculation.calculateCutoff();
    		System.out.print("Cutoff= "+ContrailConfig.cutoff);
    	}
    	
    	if(pruneCountFile)
    	{
            FilterGlobalCountFile.FilterKmers(ContrailConfig.Quake_KmerCount,ContrailConfig.Quake_Filtered_KmerCount,ContrailConfig.cutoff);

    	}
    	//////Working till here!
    	//rec.run("inputfq","outputfq");
    	
       } catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
		
    }
}
