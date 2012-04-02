package org.avrotest;

import java.io.File;
import java.io.IOException;


public class generateQuakeCompatibleBithash {

	public static void run()
	{	
		Process p;
		
		try {
			//Cleaning up older data
			File f = new File(ContrailConfig.Quake_Data+"/merged.out");
			if(f.exists()) f.delete();
			f = new File(ContrailConfig.Quake_Data+"/qcb");
			if(f.exists()) f.delete();
			
			String command = ContrailConfig.Hadoop_Home+"/bin/hadoop dfs -getmerge "+ContrailConfig.BitHash_Output+" "+ContrailConfig.Quake_Data+"/merged.out";
			 
			p = Runtime.getRuntime().exec(command);
			int i = p.waitFor();
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
				
		String writer_source = ContrailConfig.Quake_Home+"/src/write_bithash";
		
		
		
		String command = writer_source+" "+ContrailConfig.Quake_Data+"/merged.out "+ContrailConfig.Quake_Data+"/qcb";
		System.out.println(command);
		try {
			 
			p = Runtime.getRuntime().exec(command);
			 
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//output in bithashquake.out
		
	}
}
