package org.avrotest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class joinGenerator {
/* Invokes the pig script*/
	public static void generateJoin(String joinedFilePath, String cmd) throws Exception 
	{
		//delete mergedpath if it exists for the pig script 

		String delCommand = ContrailConfig.Hadoop_Home + "/bin/hadoop dfs -rmr "+joinedFilePath; 
		
		try {
			//System.out.print();
			Process p = Runtime.getRuntime().exec(delCommand);
			BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String s;
			System.out.println("Output of Pig Run");
			 while ((s = stdInput.readLine()) != null) {
				 System.out.println(delCommand);
			 }
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		String pig_file_path = null;
		String command = null;
		
		if ( cmd.equals("flash"))
		{
			pig_file_path = ContrailConfig.Script_Home+"/flashjoin.pig";
			command = "pig " + pig_file_path;
		}
		
		else
		{
			pig_file_path = ContrailConfig.Script_Home+"/quakejoin.pig";
			command = "pig "+ pig_file_path;
			
		}
		System.out.println(command);
		try {
			
			Process p = Runtime.getRuntime().exec(command);
			BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String s;
			
			 while ((s = stdInput.readLine()) != null) {
				 System.out.println(command);
			 }
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
