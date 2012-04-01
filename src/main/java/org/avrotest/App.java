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
    	boolean calculateBitHash = true;
    	boolean createGlobalBitHash = true;
    	boolean quakecompatiblebh = true;
    	boolean correctForSingles = true;
    	int K =13;
    	ContrailConfig.K = K;
    	if(kmercount)
    	{
    		kmerCounter k = new kmerCounter();
    		k.run("outputfq", "ContrailPlus/quake_kmer_count", K);
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
    	ContrailConfig.cutoff = 3; /// Remove this after testing
    	if(pruneCountFile)
    	{
            FilterGlobalCountFile.FilterKmers(ContrailConfig.Quake_KmerCount,ContrailConfig.Quake_Filtered_KmerCount,ContrailConfig.cutoff);

    	}
    	//////Working till here!
    	
    	if(calculateBitHash)
    	{
    		localBitHash.bitHashlocal(ContrailConfig.Quake_Filtered_KmerCount, ContrailConfig.junkPath, K, ContrailConfig.cutoff);
    	}
    	//rec.run("inputfq","outputfq");
    	if(createGlobalBitHash)
    	{
    		System.out.println("Generating Numeric Bithash ");
            CreateGlobalBitHash.BitMapGenerator(ContrailConfig.BitHash_Local_Temp, ContrailConfig.BitHash_Output);
    	}
    	if(quakecompatiblebh)
    	{
    		System.out.println("Generating Quake compatible Bithash");
    		generateQuakeCompatibleBithash.run();
            
    	}
    	
    	if(correctForSingles)
    	{
    		//Uncomment this after Flash is done
    		//String paths = ContrailConfig.Singles+","+ContrailConfig.Flash_Final_Out;
    		String paths = ContrailConfig.Singles;
    		CorrectSinglesInvocationStub.run(paths,ContrailConfig.junkPath);
    	}
    	
    	
       } catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
		
    }
}
