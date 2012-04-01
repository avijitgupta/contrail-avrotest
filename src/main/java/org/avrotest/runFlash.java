package org.avrotest;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class runFlash {
	
	public static void flashRunner(String filePathFq1, String filePathFq2, String localTime, String FlashFinalOut, String FlashLocalOut, String FlashHome, String HadoopHome, String CorrectInPath)
	{
		String flashPath = FlashHome;
		String FlashOutputDirectory = FlashLocalOut +"/"+ localTime+"/";
		
		String command = flashPath+"/flash "+filePathFq1+" "+filePathFq2+" -d "+ FlashOutputDirectory;
		System.out.println(command);
		
		try {
			Process p = Runtime.getRuntime().exec(command);
			BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String s;
			System.out.println("Output of Flash Run");
			 while ((s = stdInput.readLine()) != null) {
				 System.out.println(s);
				 
			 }
			 p.waitFor();
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		//These are the three output files of Flash on each machine.. The Combined File, and Two Not Combined Files..
		// Here, the Not Combined Files are discarded - not used further. - D
		String combinedFilePath = FlashOutputDirectory + "out.extendedFrags.fastq";
		String nc1 = FlashOutputDirectory + "/out.notCombined_1.fastq";
		String nc2 = FlashOutputDirectory + "/out.notCombined_2.fastq";
		
		String q1,s, flash_out, correct_in;
		
		// The Output of Flash is written to FlashFinalOut, for record.
		
		flash_out = FlashFinalOut + "/out.extendedFrags"+localTime+".fastq";
		//It is copied to 
		correct_in = CorrectInPath+ "/out.extendedFrags"+localTime+".fastq";
		//uniquenc1 = FlashOutputDirectory + "/out.notCombined_1"+localTime+".fastq";
		//uniquenc2 = FlashOutputDirectory + "/out.notCombined_2"+localTime+".fastq";
		Process p;
		BufferedReader stdInput;
		try
		{
				
			 Configuration conf2 = new Configuration();
			    FileSystem fs = FileSystem.get(conf2);
			    //Deleting output path if exists
			    Path fp1 = new Path(combinedFilePath);
			    Path fp2 = new Path(flash_out);
			    Path fp3 = new Path(correct_in);
			    fs.copyFromLocalFile(fp1,fp2);
			    fs.moveFromLocalFile(fp1,fp3);
					/*
					q1= HadoopHome+"/bin/hadoop dfs -copyFromLocal "+combinedFilePath+" "+flash_out;
					System.out.println(q1);
					p = Runtime.getRuntime().exec(q1);
					stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
					System.out.println("Output of Flash written to:"+ FlashOutputDirectory);
					while ((s = stdInput.readLine()) != null) {
					System.out.println(s);
					}
					p.waitFor();
					q1= HadoopHome+"/bin/hadoop dfs -copyFromLocal "+combinedFilePath+" "+correct_in;
					System.out.println(q1);
					p = Runtime.getRuntime().exec(q1);
					stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
					System.out.println("Output combined file - Flash's output Copied to Correct's Input:"+CorrectInPath);
					while ((s = stdInput.readLine()) != null) {
					System.out.println(s);
					}
					p.waitFor();*/
				 
				
				
			
		}
		catch(Exception e){System.out.println(e.toString());}
		
		
		//cleanup. After the files have been copied to HDF, delete the local data created on the node's local FS.
	/*	File fp = new File(combinedFilePath);
		if(fp.exists())fp.delete();
		fp = new File(nc1);
		if(fp.exists())fp.delete();
		fp = new File(nc2);
		if(fp.exists())fp.delete();
		
		//Delete Final Directory
		fp = new File(FlashOutputDirectory);
		if(fp.exists())fp.delete();*/
	}

}
