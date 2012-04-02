package org.avrotest;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CorrectLocal
{
	static void runcode(String filePath, String localTime, long K , String hadoophome, String quakehome, String quakedata, String singles_out)
	{
		
			String fastqLocation = filePath;
			String fastqListLocation = quakedata+"/"+localTime+".txt";
			
			try{
				/* Writes the temporary file to be created - its name to the temp .txt file - fastqListLocation*/
				  FileWriter fstream = new FileWriter(fastqListLocation,true);
				  BufferedWriter out = new BufferedWriter(fstream);
				  out.write(fastqLocation+"\n");
				  out.close();
				  fstream.close();
				}
			
				  catch (Exception e)	{e.printStackTrace();}
			
				  	String bitHashLocation = quakedata+"/qcb";
					String CorrectLocation = quakehome+"/src/correct";				
					String q=CorrectLocation+" -f "+ fastqListLocation + " -k " + Long.toString(K) + " -b "+bitHashLocation;
					System.out.println(q);


				  	try {
						Process p = Runtime.getRuntime().exec(q);
						int i = p.waitFor();

						BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
						String s;
						System.out.println("Correct run");
						 while ((s = stdInput.readLine()) != null) {
							 System.out.println(s);
						 }
					
						
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
						String tempPath = fastqLocation.substring(0,fastqLocation.lastIndexOf('.'));
						  		  	
						String correctedFilePath = tempPath + ".cor.fq";
						
						try
						{
						
				  			    Configuration conf2 = new Configuration();
							    FileSystem fs = FileSystem.get(conf2);
							    //Deleting output path if exists
							    Path fp1 = new Path(correctedFilePath);
							    Path fp2 = new Path("ContrailPlus/Quake_Singles_Out/");
							    fs.moveFromLocalFile(fp1, fp2);
							  
						
						}
						catch(Exception e){System.out.println(e.toString());}
					
						File fp = new File(fastqListLocation);  
					     if(fp.exists())fp.delete();
					
		
	} 
}
