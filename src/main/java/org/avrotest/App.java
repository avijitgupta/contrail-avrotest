package org.avrotest;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;


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
    	//ContrailConfig.readXMLConfig();
    	ContrailConfig.readXMLConfig();
    	boolean simulateJoin = false;
    	boolean kmercount =false;
    	boolean avroToNonAvroPartFile = false;
    	boolean calculateCutOff = false;
    	boolean pruneCountFile = false;
    	boolean calculateBitHash = false;
    	boolean createGlobalBitHash = false;
    	boolean quakecompatiblebh = false;
    	boolean correctForSingles = false;
    	boolean runFlash = true;
    	boolean run_paired_quake = true;
    	int K =13;
    	ContrailConfig.K = K;
    	if(simulateJoin)
    	{
    		JoinSimulator js = new JoinSimulator();
    		js.run(ContrailConfig.Singles, ContrailConfig.Flash_Join_Out);
    	}
    	
    	if(runFlash)
    	{
    		String command = ContrailConfig.Hadoop_Home+"/bin/hadoop dfs -rmr "+ContrailConfig.Flash_Final_Out;

        	try {
                 
                 Process p = Runtime.getRuntime().exec(command);
                 BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
                 String s;
                  while ((s = stdInput.readLine()) != null) {
                      System.out.print("");
                  }
                
             } catch (IOException e) {
                 // TODO Auto-generated catch block
                 e.printStackTrace();
             }
             
             String command2 = ContrailConfig.Hadoop_Home+"/bin/hadoop dfs -mkdir "+ContrailConfig.Flash_Final_Out;
             

             try {
                 
                 Process p = Runtime.getRuntime().exec(command2);
                 BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
                 
                 String s;

                  while ((s = stdInput.readLine()) != null) {
                      System.out.print("");
                  }
                
             } catch (IOException e) {
                 // TODO Auto-generated catch block
                 e.printStackTrace();
             }
             System.out.println("Flashed Paired Files created");
             // This is the key method here ExportToPairedFiles() - It triggers a MapReduce Job to first write flash input data on each node, 
         	 //and then runs flash across the cluster.
             createPairedReadsForFlash.run(ContrailConfig.Flash_Join_Out, ContrailConfig.junkPath);
    	}
    	
    	
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
    	
    	if(run_paired_quake)
    	{
    		String command = ContrailConfig.Hadoop_Home+"/bin/hadoop dfs -ls "+ContrailConfig.Quake_Final_Out;
         	
         	try {
                  
                  Process p = Runtime.getRuntime().exec(command);
                  BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
                  String s;
            
                   while ((s = stdInput.readLine()) != null) {
                       System.out.print("");
                   }
                 
              } catch (IOException e) {
                  // TODO Auto-generated catch block
                  e.printStackTrace();
              }
         	System.out.println("Quaked Paired Files creation");
         	createPairedReadsForQuake.createPairedReads(ContrailConfig.Quake_Join_Out, ContrailConfig.junkPath);
    	}
    	
    	
       } catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
		
    }
}
