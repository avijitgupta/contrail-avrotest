package org.avrotest;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
public class runCorrectForPairedReads {
	public static void correctRunner(String filePathFq1, String filePathFq2, String localTime, long K, String quakehome, String quakedata, String quake_mates_out, String hadoophome)
	{
				
		String bitHashLocation = quakedata+"/qcb";
		String CorrectLocation = quakehome+"/src/correct";
		String fastqListLocation = quakedata+"/"+localTime+".txt";
		
		try{
			  FileWriter fstream = new FileWriter(fastqListLocation,true);
			  BufferedWriter out = new BufferedWriter(fstream);
			  out.write(filePathFq1+" "+filePathFq2+"\n");
			  out.close();
			  fstream.close();
			}
			  catch (Exception e)	{e.printStackTrace();}
		
		String q=CorrectLocation+" -f "+ fastqListLocation + " -k " + Long.toString(K) + " -b "+bitHashLocation;
		System.out.println(q);

	  	try {
			Process p = Runtime.getRuntime().exec(q);
			BufferedReader stdInput = new BufferedReader(new 
		            InputStreamReader(p.getInputStream()));
			String s;
			System.out.println("Output");
			 while ((s = stdInput.readLine()) != null) {
				 System.out.println(s);
			 }
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
		try
		{
			  	String tempPath1 = filePathFq1.substring(0,filePathFq1.lastIndexOf('.'));
			  	String tempPath2 = filePathFq2.substring(0,filePathFq2.lastIndexOf('.'));
			  	
			  	String correctedFilePath1 = tempPath1 + ".cor.fq";
			  	String correctedFilePath2 = tempPath2 + ".cor.fq";
				
			  	 Configuration conf2 = new Configuration();
				    FileSystem fs = FileSystem.get(conf2);
				    //Deleting output path if exists
				    Path fp1 = new Path(correctedFilePath1);
				    Path fp2 = new Path(correctedFilePath2);
				    Path fp3 = new Path(quake_mates_out);
				    fs.moveFromLocalFile(fp1, fp3);
				    fs.moveFromLocalFile(fp2, fp3);
				    
				    //Clean Local files
				    File f = new File(correctedFilePath1);
					if (f.exists()) f.delete();
					f = new File(correctedFilePath2);
					if (f.exists()) f.delete();
			
		}
		catch(Exception e){System.out.println(e.toString());}
		
		File fp = new File(fastqListLocation);
		if(fp.exists())fp.delete();
		
	}
}
