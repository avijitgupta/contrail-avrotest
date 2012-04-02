package org.avrotest;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

/*
 * This class calculates the cutoff by invoking cov_model.py - Quake
 */
public class cutOffCalculation {

	public static int calculateCutoff() throws Exception{
		String Hadoop_Home = ContrailConfig.Hadoop_Home+"/";
		String Quake_Path = ContrailConfig.Quake_Home+"/";
		String Kmer_Directory = ContrailConfig.Quake_Data;
		File fp;
		
		//We only need one part file to calculate the cutoff..
		String Hadoop_Kmer_Output0 = ContrailConfig.Non_Avro_Count_File+"/part-00000";
		
		File f = new File(ContrailConfig.Quake_Data+"/part-00000");
		if (f.exists()) f.delete();
		
		String command = Hadoop_Home+"bin/hadoop dfs -copyToLocal "+Hadoop_Kmer_Output0+" "+Kmer_Directory;
		//copy the part file to client node's Quake Data directory
		
		System.out.println("Cutoff Calculation copy: "+ command);
		
		
		String s;
		Process p = Runtime.getRuntime().exec(command);
		p.waitFor();
		BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
        BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));
        StringTokenizer str ;
	    
	        int cutoff=0;
	       
	        
	        while ((s = stdInput.readLine()) != null) {
	                  	System.out.println(s);
	        }

		String command2 = Quake_Path+"bin/cov_model.py --int "+Kmer_Directory+"/part-00000";
		
		System.out.println("Cutoff Calculation: "+ command2);
		
		p = Runtime.getRuntime().exec(command2);
		
		stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
        stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));

        System.out.println("Running cov_model.py\n");
        while ((s = stdInput.readLine()) != null) {
         str = new StringTokenizer(s);
            
        /*This is a hack - everything displayed by the execution of 
         * cov_model.py here is stored in str line by line. In the end, the token containing
         * the cutoff is taken out :P
         * */
           if(str.countTokens()!=0 && str.nextToken().trim().equals("Cutoff:"))
            {
            		String ss = str.nextToken();
            		cutoff = Integer.parseInt(ss);
            		break;
            }
        
        }
		p.waitFor();
        //Deleting Temp File	
		f = new File(ContrailConfig.Quake_Data+"/part-00000");
		if (f.exists()) f.delete();
		
	return cutoff;	
	}
	
	
}
