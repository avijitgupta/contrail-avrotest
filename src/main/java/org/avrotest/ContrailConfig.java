package org.avrotest;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Map.Entry;

import javax.naming.ConfigurationException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.commons.configuration.XMLConfiguration;

public class ContrailConfig 
{
	// important paths
	public static String hadoopReadPath = null;
	public static String hadoopBasePath = null;
	public static String localBasePath = "work";
	
	// FLASH and QUAKE variables
	///////////////////////////////////////////////////
	static HashMap<String,String> manifestMap = new HashMap();
	// Setup Paths - To be Read from XML Config - deepak
	
	public static String Flash_Home = null;
	public static String Quake_Home = null;
	public static String Hadoop_Home = null;
	public static String Quake_Data = null;

	// HDFS Input Directory of Set 1 for Flash
	public static String Flash_Mate_1 = null;
	// Flattened Data from Set 1
	public static String Flash_Mate_1_Flat = null;
	
	//HDFS Input Directory of Set 2 (Set 1's Mates) for Flash
	public static String Flash_Mate_2 = null;
	//Flattened Data from Set 2
	public static String Flash_Mate_2_Flat = null;
	
	//Path on each node to its local Flash Input Data
	//Batched inputs for Flash Merging will be created at this path.
	
	public static String Flash_Node_In = null;
	//and output of flash at this path on each node..
	public static String Flash_Node_Out = null;
	public static String junkPath = null;
	
	// output of map-side join data..
	public static String Flash_Join_Out = null;
	public static String Quake_Join_Out = null;
	
	public static String Script_Home = null;
	public static String Quake_Node_In_Data = null;
	public static String Quake_Node_Out_Data = null;
	public static String Manifest = null;
	public static String Singles = null;
	
	public static String Quake_Mate_1 = null;
	public static String Quake_Mate_2 = null;
	
	public static String Quake_Mate_1_Flat = null;
	public static String Quake_Mate_2_Flat = null;
	
	public static String Quake_Final_Out = null;
	public static String Quake_Singles_Out = null;
	
	public static String Flash_Final_Out = null;
	
	public static String Quake_Preprocess = null;
	public static String Quake_KmerCount = null;
	public static String Quake_Filtered_KmerCount = null;
	public static String BitHash_Input = null;
	public static String BitHash_Output = null;
	public static String BitHash_Local_Temp = null;
	public static String Quake_Mates_Out = null;
////////////////////////////////////////////////////
	
	// hadoop options
	public static int    HADOOP_MAPPERS    = 50;
	public static int    HADOOP_REDUCERS   = 50;
	public static int    HADOOP_LOCALNODES = 1000;
	public static long   HADOOP_TIMEOUT    = 3600000;
	public static String HADOOP_JAVAOPTS   = "-Xmx1000m";
	
	// Assembler options
	public static String STARTSTAGE = null;
	public static String STOPSTAGE = null;
	
	// restart options
	public static boolean validateonly = false;
	public static boolean forcego = false;
	public static boolean RESTART_USED = false;
	public static int RESTART_INITIAL = 0;
	public static int RESTART_TIP = 0;
	public static int RESTART_TIP_REMAIN = 0;
	public static int RESTART_COMPRESS = 0;
	public static int RESTART_COMPRESS_REMAIN = 0;
	
	public static String RESTART_SCAFF_STAGE = null;
	public static long   RESTART_SCAFF_FRONTIER = 0;
	public static int    RESTART_SCAFF_PHASE = 0;
	
	// initial node construction
	public static long K = -1;
	public static long MAXR5 = 250;
	public static long MAXTHREADREADS = 250;
	public static int  RECORD_ALL_THREADS = 0;

	// hard trim
	public static long TRIM3 = 0;
	public static long TRIM5 = 0;
	
	// tips
	public static long TIPLENGTH = -3;
	
	// bubbles
	public static long MAXBUBBLELEN    = -5;
	public static float BUBBLEEDITRATE = 0.05f;

	// low cov
	public static float LOW_COV_THRESH  = 5.0f;
	public static long  MAX_LOW_COV_LEN = -2;
	static int cutoff;
	
	// threads
	public static long  MIN_THREAD_WEIGHT = 5;

    // scaffolding
	public static long  INSERT_LEN     = 0;
	public static long  MIN_WIGGLE     = 30;
	public static float MIN_UNIQUE_COV = 0;
	public static float MAX_UNIQUE_COV = 0;
	public static long  MIN_CTG_LEN    = -1;
	public static long  MAX_FRONTIER   = 10;
	
	// stats
	public static String RUN_STATS = null;
	public static long  N50_TARGET = -1;
	public static String CorrectInDirectory = null;
	
    // conversion	
	public static String CONVERT_FA = null;
	public static String PRINT_FA = null;
	public static long FASTA_MIN_LEN = 100;
	public static float FASTA_MIN_COV = 2;
	
	// preprocess
	public static long   PREPROCESS_SUFFIX = 0;
	
	
	public static boolean TEST_MODE = false;
	
	public static void validateConfiguration()
	{
		// negative values are code for use a function of K
		if (TIPLENGTH < 0)         { TIPLENGTH       *= -K; }
		if (MAXBUBBLELEN < 0)      { MAXBUBBLELEN    *= -K; }
		if (MAX_LOW_COV_LEN < 0)   { MAX_LOW_COV_LEN *= -K; }
		if (MIN_CTG_LEN < 0)       { MIN_CTG_LEN     *= -K; }
		
		int err = 0;
		
		if (TEST_MODE)
		{
			System.err.println("TEST_MODE");
		}
		else if ((RUN_STATS == null) && (CONVERT_FA == null) && (PRINT_FA == null))
		{
			if (hadoopBasePath == null) { err++; System.err.println("ERROR: -asm is required"); }
			if (K <= 0)                 { err++; System.err.println("ERROR: -k is required"); }
			if (STARTSTAGE == null && hadoopReadPath == null) { err++; System.err.println("ERROR: -reads is required"); }
		
			if (INSERT_LEN > 0)
			{
				if (MIN_UNIQUE_COV <= 0) { err++; System.err.println("ERROR: -minuniquecov is required when insertlen > 0"); }
				if (MAX_UNIQUE_COV <= 0) { err++; System.err.println("ERROR: -maxuniquecov is required when insertlen > 0"); }
			}
			
			if (RESTART_USED && STARTSTAGE == null)
			{
				err++;
				System.err.println("ERROR: Restart options used, but starting from graph construction"); 
			}

			if (RESTART_SCAFF_STAGE != null)
			{
				if (!RESTART_SCAFF_STAGE.equals("edges") &&
						!RESTART_SCAFF_STAGE.equals("bundles") &&
						!RESTART_SCAFF_STAGE.equals("frontier") &&
						!RESTART_SCAFF_STAGE.equals("update") &&
						!RESTART_SCAFF_STAGE.equals("finalize") &&
						!RESTART_SCAFF_STAGE.equals("clean") &&
						!RESTART_SCAFF_STAGE.equals("threadrepeats") &&
						!RESTART_SCAFF_STAGE.equals("threadible") &&
						!RESTART_SCAFF_STAGE.equals("resolve") &&
						!RESTART_SCAFF_STAGE.equals("compress") &&
						!RESTART_SCAFF_STAGE.equals("removetips") &&
						!RESTART_SCAFF_STAGE.equals("popbubbles"))
				{
					err++; 
					System.err.println("ERROR: Unknown stage specified for -restart_scaff_stage");
				}
			}
		
			if (err > 0) { System.exit(1); }
		}

		if ((hadoopBasePath != null) && (!hadoopBasePath.endsWith("/"))) { hadoopBasePath += "/"; }
		if ((localBasePath != null) && (!localBasePath.endsWith("/")))  { localBasePath += "/"; }
	}
	
     //  READING XML 
	/////////////////////////////////////////////////////
	
	public static void readXMLConfig() throws Exception
	{
		
		//Present working directory
		String pwd = System.getProperty("user.dir");
	
		XMLConfiguration config = new XMLConfiguration("/home/hduser/workspace/contrailPlus/config/config.xml");	
		Flash_Home = config.getString("setup.flash_home");
		Quake_Home = config.getString("setup.quake_home");
		Hadoop_Home = config.getString("setup.hadoop_home");
		Script_Home = config.getString("setup.pig_script");
		
		Flash_Mate_1 = config.getString("hdfs_in_data.flash_mate_1");
		Flash_Mate_2 = config.getString("hdfs_in_data.flash_mate_2");
		Quake_Mate_1 = config.getString("hdfs_in_data.quake_mate_1");
		Quake_Mate_2 = config.getString("hdfs_in_data.quake_mate_2");
		Flash_Mate_1_Flat = config.getString("hdfs_out_data.flash_mate_1_flat");
		Flash_Mate_2_Flat = config.getString("hdfs_out_data.flash_mate_2_flat");
		Quake_Mate_1_Flat = config.getString("hdfs_out_data.quake_mate_1_flat");
		Quake_Mate_2_Flat = config.getString("hdfs_out_data.quake_mate_2_flat");
		Singles = config.getString("hdfs_in_data.singles");
		Quake_Singles_Out = config.getString("hdfs_out_data.quake_singles_out");
		Quake_Final_Out = config.getString("hdfs_out_data.quake_final_out");
		
		Flash_Node_In = config.getString("local_data.flash_input");
		
		Flash_Node_Out = config.getString("local_data.flash_output");
		
		 
		Flash_Join_Out = config.getString("hdfs_out_data.flash_join_out");
		Flash_Final_Out = config.getString("hdfs_out_data.flash_final_out");
		Quake_Join_Out = config.getString("hdfs_out_data.quake_join_out");
		Quake_Mates_Out = config.getString("hdfs_out_data.quake_mates_out");
		
		junkPath = config.getString("hdfs_out_data.junk");
		Manifest = config.getString("local_data.manifest");
		
		// This method reads the Manifest File and Adds an Extra Column of Unique IDs.
		//This extra column helps in the MapSide Join by giving a column of unique IDs to join on.
		manifestRead();
		
		// the HDFS Input Directory for Running Quake's Correct.
		CorrectInDirectory = config.getString("hdfs_in_data.correctInputDirectory");
		Quake_Data = config.getString("local_data.quake_data");
		Quake_Preprocess = config.getString("hdfs_out_data.quake_preprocess");
		Quake_KmerCount = config.getString("hdfs_out_data.quake_kmercount");
		Quake_Filtered_KmerCount = config.getString("hdfs_out_data.quake_filtered_kmercount");
		BitHash_Output = config.getString("hdfs_out_data.bithash_output");
		BitHash_Local_Temp = config.getString("hdfs_out_data.bithash_local_temp");
	}
	
	public static void manifestRead() throws IOException
	{
		String manifestPath = Manifest;
		int count = 0;
		StringTokenizer st;
		String line;
		char uniqueString[] = new char[5];
		uniqueString[0]='a';
		uniqueString[1]='a';
		uniqueString[2]='a';
		uniqueString[3]='a';
		uniqueString[4]='a';
		int n=0;
		
		try
		{
			BufferedReader bf = new BufferedReader(new InputStreamReader(new FileInputStream(manifestPath)));
			//System.out.print(bf.toString());
			do {
				line = bf.readLine();
				if(line == null)break;
				count ++;
				if(count ==1)continue;
				st = new StringTokenizer(line);
				String fname1 = st.nextToken();
				String fname2 = st.nextToken();
				String overlap = st.nextToken();
				String do_flash = st.nextToken();
				String do_quake = st.nextToken();
				//System.out.print(fname1+"\n");
					int charToBeEdited = 4 - n/26;
					
					uniqueString[charToBeEdited] = (char)((int)(uniqueString[charToBeEdited]) +1); 
					
					String str ="";
					
					for(int i=0;i<5;i++)str+=uniqueString[i];
					String temp = overlap+"\t"+do_flash+"\t"+do_quake+"\t"+str+"\n";
					
					manifestMap.put(new String(fname1.trim()), temp);
					manifestMap.put(new String(fname2.trim()), temp);
					
					n++;
					
					
					
				
			}while(line != null);
		}
		catch(Exception e){}
		
		 File fp = new File(Manifest+"_temp");
		 if(fp.exists())fp.delete();

		 FileWriter fstream = new FileWriter(Manifest+"_temp",true);
		  BufferedWriter out = new BufferedWriter(fstream);
		  for(String key: manifestMap.keySet())
		  {
			  
			  String uniqueName = manifestMap.get(key);
			  out.write(key+"\t"+uniqueName);
			  //System.out.println(key+"\t"+uniqueName);
		  }
		  out.close();
		  fstream.close();
		
		 
		//  fp = new File(Manifest+"_temp");
		 // fp.renameTo(new File(Manifest));
	
	}
////////////////////////////////////////////////////////////////
	
	
	public static void initializeConfiguration(JobConf conf)
	{
		validateConfiguration();
		
		conf.setNumMapTasks(HADOOP_MAPPERS);
		conf.setNumReduceTasks(HADOOP_REDUCERS);
		conf.set("mapred.child.java.opts", HADOOP_JAVAOPTS);
		conf.set("mapred.task.timeout", Long.toString(HADOOP_TIMEOUT));
		
		conf.setLong("LOCALNODES", HADOOP_LOCALNODES);
		
	    conf.setLong("PREPROCESS_SUFFIX", PREPROCESS_SUFFIX);
		
		conf.setLong("K", K);
		conf.setLong("MAXTHREADREADS", MAXTHREADREADS);
		conf.setLong("MAXR5", MAXR5);
		conf.setLong("RECORD_ALL_THREADS", RECORD_ALL_THREADS);
		
		conf.setLong("TRIM5", TRIM5);
		conf.setLong("TRIM3", TRIM3);
		
		conf.setLong("TIPLENGTH", TIPLENGTH);

		conf.setLong("MAXBUBBLELEN", MAXBUBBLELEN);
		conf.setFloat("BUBBLEEDITRATE", BUBBLEEDITRATE);
		
		conf.setFloat("LOW_COV_THRESH", LOW_COV_THRESH);
		conf.setLong("MAX_LOW_COV_LEN", MAX_LOW_COV_LEN);
		
		conf.setLong("MIN_THREAD_WEIGHT", MIN_THREAD_WEIGHT);
		
		conf.setLong("INSERT_LEN", INSERT_LEN);
		conf.setLong("MIN_CTG_LEN", MIN_CTG_LEN);
		conf.setLong("MIN_WIGGLE", MIN_WIGGLE);
		conf.setFloat("MIN_UNIQUE_COV", MIN_UNIQUE_COV);
		conf.setFloat("MAX_UNIQUE_COV", MAX_UNIQUE_COV);
		
		conf.setLong("N50_TARGET", N50_TARGET);
		
		conf.setLong("FASTA_MIN_LEN", FASTA_MIN_LEN);
		conf.setFloat("FASTA_MIN_COV", FASTA_MIN_COV);
	}
	
	
	public static void printConfiguration()
	{
		validateConfiguration();
		/*
		Contrail.msg("Contrail " + Contrail.VERSION + "\n");
		Contrail.msg("==================================================================================\n");
		Contrail.msg("Input: "         + hadoopReadPath + "\n");
		Contrail.msg("Workdir: "       + hadoopBasePath  + "\n");
		Contrail.msg("localBasePath: " + localBasePath + "\n");
		
		Contrail.msg("HADOOP_MAPPERS = "    + HADOOP_MAPPERS + "\n");
		Contrail.msg("HADOOP_REDUCERS = "   + HADOOP_REDUCERS + "\n");
		Contrail.msg("HADOOP_JAVA_OPTS = "  + HADOOP_JAVAOPTS + "\n");
		Contrail.msg("HADOOP_TIMEOUT = "    + HADOOP_TIMEOUT + "\n");
		Contrail.msg("HADOOP_LOCALNODES = " + HADOOP_LOCALNODES + "\n");
		
		if (STARTSTAGE != null)  { Contrail.msg("STARTSTAGE = " + STARTSTAGE + "\n"); }
		if (STOPSTAGE  != null) { Contrail.msg("STOPSTAGE = "  + STOPSTAGE + "\n");  }
		
		Contrail.msg("RESTART_INITIAL = " + RESTART_INITIAL + "\n");
			
		Contrail.msg("RESTART_TIP = " + RESTART_TIP + "\n");
		Contrail.msg("RESTART_TIP_REMAIN = " + RESTART_TIP_REMAIN + "\n");
		
		Contrail.msg("RESTART_COMPRESS = " + RESTART_COMPRESS + "\n");
		Contrail.msg("RESTART_COMPRESS_REMAIN = " + RESTART_COMPRESS_REMAIN + "\n");
		
		Contrail.msg("RESTART_SCAFF_PHASE = " + RESTART_SCAFF_PHASE + "\n");
		Contrail.msg("RESTART_SCAFF_STAGE = " + RESTART_SCAFF_STAGE + "\n");
		Contrail.msg("RESTART_SCAFF_FRONTIER = " + RESTART_SCAFF_FRONTIER + "\n");
		
		Contrail.msg("PREPROCESS_SUFFIX = " + PREPROCESS_SUFFIX + "\n");

		Contrail.msg("K = "               + K + "\n");
		Contrail.msg("MAXR5 = "           + MAXR5 + "\n");
		Contrail.msg("MAXTHREADREADS = "  + MAXTHREADREADS + "\n");
		Contrail.msg("TRIM5 = "           + TRIM5 + "\n");
		Contrail.msg("TRIM3 = "           + TRIM3 + "\n");
		Contrail.msg("RECORD_ALL_THREADS = " + RECORD_ALL_THREADS + "\n");

		Contrail.msg("TIPLENGTH = "       + TIPLENGTH + "\n");

		Contrail.msg("MAXBUBBLELEN = "    + MAXBUBBLELEN + "\n");
		Contrail.msg("BUBBLEEDITRATE = "  + BUBBLEEDITRATE + "\n");
		
		Contrail.msg("LOW_COV_THRESH = "  + LOW_COV_THRESH + "\n");
		Contrail.msg("MAX_LOW_COV_LEN = " + MAX_LOW_COV_LEN + "\n");
		
		Contrail.msg("MIN_THREAD_WEIGHT = " + MIN_THREAD_WEIGHT + "\n");
		
		Contrail.msg("INSERT_LEN = "     + INSERT_LEN + "\n");
		Contrail.msg("MIN_WIGGLE = "     + MIN_WIGGLE + "\n");
		Contrail.msg("MIN_CTG_LEN = "    + MIN_CTG_LEN + "\n");
		Contrail.msg("MIN_UNIQUE_COV = " + MIN_UNIQUE_COV + "\n");
		Contrail.msg("MAX_UNIQUE_COV = " + MAX_UNIQUE_COV + "\n");
		Contrail.msg("MAX_FRONTIER = "   + MAX_FRONTIER + "\n");
		
		Contrail.msg("RUN_STATS = " + RUN_STATS + "\n");
		Contrail.msg("N50_TARGET = " + N50_TARGET + "\n");
		
		Contrail.msg("CONVERT_FA = " + CONVERT_FA + "\n");
		Contrail.msg("PRINT_FA = " + PRINT_FA + "\n");
		Contrail.msg("FASTA_MIN_LEN = " + FASTA_MIN_LEN + "\n");
		Contrail.msg("FASTA_MIN_COV = " + FASTA_MIN_COV + "\n");
		
		Contrail.msg("\n");
		
		if (validateonly && !forcego)
		{
			System.exit(0);
		}*/
	}
	
	public static void printJobConf(JobConf job)
	{
		System.err.println("job: " + job.toString());
		Iterator<Entry<String, String>> i = job.iterator();
		
		while (i.hasNext())
		{
			Entry<String, String> e = i.next();
			System.err.println(e.getKey() + " => " + e.getValue());
		}
	}
	
	public static void parseOptions(String [] args)
	{
		Options options = new Options();
		
		options.addOption(new Option("help",     "print this message"));
		options.addOption(new Option("h",        "print this message"));
		options.addOption(new Option("expert",   "show expert options"));
		options.addOption(new Option("validate", "validate and print options"));
		options.addOption(new Option("go",       "go even when validating"));
		
		// work directories
		options.addOption(OptionBuilder.withArgName("hadoopBasePath").hasArg().withDescription("Base Hadoop assembly directory [required]").create("asm"));
		options.addOption(OptionBuilder.withArgName("hadoopReadPath").hasArg().withDescription("Hadoop read directory [required]").create("reads"));
		options.addOption(OptionBuilder.withArgName("workdir").hasArg().withDescription("Local work directory (default: " + localBasePath + ")").create("work"));

		// hadoop options
		options.addOption(OptionBuilder.withArgName("numSlots").hasArg().withDescription("Number of machine slots to use (default: " + HADOOP_MAPPERS + ")").create("slots"));
		options.addOption(OptionBuilder.withArgName("numNodes").hasArg().withDescription("Max nodes in memory (default: " + HADOOP_LOCALNODES + ")").create("nodes"));
		options.addOption(OptionBuilder.withArgName("childOpts").hasArg().withDescription("Child Java Options (default: " + HADOOP_JAVAOPTS + ")").create("javaopts"));
		options.addOption(OptionBuilder.withArgName("millisecs").hasArg().withDescription("Hadoop task timeout (default: " + HADOOP_TIMEOUT + ")").create("timeout"));

		
		// job restart
		options.addOption(OptionBuilder.withArgName("stage").hasArg().withDescription("Starting stage").create("start"));
		options.addOption(OptionBuilder.withArgName("stage").hasArg().withDescription("Stop stage").create("stop"));
		
		options.addOption(new Option("restart_initial", "restart after build initial, before quickmerge"));
		
		options.addOption(OptionBuilder.withArgName("stage").hasArg().withDescription("Last completed compression stage [expert]").create("restart_compress"));
		options.addOption(OptionBuilder.withArgName("cnt").hasArg().withDescription("Number remaining nodes [expert]").create("restart_compress_remain"));
		
		options.addOption(OptionBuilder.withArgName("stage").hasArg().withDescription("Last completed tip removal [expert]").create("restart_tip"));
		options.addOption(OptionBuilder.withArgName("cnt").hasArg().withDescription("Number remaining tips [expert]").create("restart_tip_remain"));
		
		
		options.addOption(OptionBuilder.withArgName("phase").hasArg().withDescription("last completed phase").create("restart_scaff_phase"));
		options.addOption(OptionBuilder.withArgName("stage").hasArg().withDescription("restart at this stage").create("restart_scaff_stage"));
		options.addOption(OptionBuilder.withArgName("hops").hasArg().withDescription("last completed frontier hop").create("restart_scaff_frontier"));
		

		// initial graph
		options.addOption(OptionBuilder.withArgName("k").hasArg().withDescription("Graph nodes size [required]").create("k"));
		options.addOption(OptionBuilder.withArgName("max reads").hasArg().withDescription("max reads starts per node (default: " + MAXR5 +")").create("maxr5"));
		options.addOption(OptionBuilder.withArgName("3' bp").hasArg().withDescription("Chopped bases (default: " + TRIM3 +")").create("trim3"));
		options.addOption(OptionBuilder.withArgName("5' bp").hasArg().withDescription("Chopped bases (default: " + TRIM5 + ")").create("trim5"));
		options.addOption(new Option("record_all_threads",  "record threads even on non-branching nodes"));

		// error correction
		options.addOption(OptionBuilder.withArgName("tip bp").hasArg().withDescription("max tip trim length (default: " + -TIPLENGTH +"K)").create("tiplen"));
		options.addOption(OptionBuilder.withArgName("bubble bp").hasArg().withDescription("max bubble length (default: " + -MAXBUBBLELEN + "K)").create("bubblelen"));
		options.addOption(OptionBuilder.withArgName("bubble erate").hasArg().withDescription("max bubble error rate (default: " + BUBBLEEDITRATE + ")").create("bubbleerate"));
		options.addOption(OptionBuilder.withArgName("min cov").hasArg().withDescription("cut nodes below min cov (default: " + LOW_COV_THRESH +")").create("lowcov"));
		options.addOption(OptionBuilder.withArgName("max bp").hasArg().withDescription("longest nodes to cut (default: " + -MAX_LOW_COV_LEN +"K)").create("lowcovlen"));

		// thread repeats
		options.addOption(OptionBuilder.withArgName("min reads").hasArg().withDescription("min number of threading reads (default: " + MIN_THREAD_WEIGHT + ")").create("threads"));
		options.addOption(OptionBuilder.withArgName("max reads").hasArg().withDescription("max reads to thread a node (default: " + MAXTHREADREADS +")").create("maxthreads"));
		
		// scaffolding
		options.addOption(OptionBuilder.withArgName("insert bp").hasArg().withDescription("insert len [required]").create("insertlen"));
		options.addOption(OptionBuilder.withArgName("wiggle bp").hasArg().withDescription("wiggle in stdev (default: " + MIN_WIGGLE + ")").create("wiggle"));
		options.addOption(OptionBuilder.withArgName("min bp").hasArg().withDescription("scaffold min unique length (default: " + -MIN_CTG_LEN + "K)").create("minuniquelen"));
		options.addOption(OptionBuilder.withArgName("min cov").hasArg().withDescription("scaffold min unique coverage (default: " + MIN_UNIQUE_COV + ")").create("minuniquecov"));
		options.addOption(OptionBuilder.withArgName("max cov").hasArg().withDescription("scaffold max unique coverage (default: " + MAX_UNIQUE_COV + ")").create("maxuniquecov"));
		options.addOption(OptionBuilder.withArgName("num stages").hasArg().withDescription("scaffold max frontier hops (default: " + MAX_FRONTIER + ")").create("maxfrontier"));
		
		//stats
		options.addOption(OptionBuilder.withArgName("dir").hasArg().withDescription("compute stats").create("run_stats"));
		options.addOption(OptionBuilder.withArgName("genome bp").hasArg().withDescription("genome size for N50").create("genome"));
		
		//convert
		options.addOption(OptionBuilder.withArgName("convertfa").hasArg().withDescription("convert fa").create("convert_fa"));
		options.addOption(OptionBuilder.withArgName("printfa").hasArg().withDescription("print fa").create("print_fa"));
		options.addOption(OptionBuilder.withArgName("fasta_min_len").hasArg().withDescription("min len for fasta (default: " + FASTA_MIN_LEN + ")").create("fasta_min_len"));
		options.addOption(OptionBuilder.withArgName("fasta_min_cov").hasArg().withDescription("min cov for fasta (default: " + FASTA_MIN_COV + ")").create("fasta_min_cov"));
		
		//preprocess
		options.addOption(OptionBuilder.withArgName("convertfa").hasArg().withDescription("convert fa").create("convert_fa"));
		
		
	    CommandLineParser parser = new GnuParser();
	    
	    try 
	    {
	        CommandLine line = parser.parse( options, args );
	        
	        if (line.hasOption("help") || line.hasOption("h") || line.hasOption("expert"))
	        {
	        	System.out.print("Contrail version " + /*Contrail.VERSION +*/ " (Schatz et al. http://contrail-bio.sf.net)\n" +
	        			         "\n" +
	        			         "Usage: Contrail [-asm dir] [-reads dir] [-k k] [options]\n" +
	        			         "\n" +
	        			         "Contrail Stages\n" +
	        	                 "================\n" +
	        	                 "preprocess : convert from fastq format\n" +
	        	                 "  -filesuffix         : use filename suffix (_1 or _2) as readname suffix\n" +
	        	                 "\n" +
	        	                 "buildInitial : build initial de Bruijn graph\n" +
	        	                 "  -k <bp>             : Initial graph node length\n" +
	        	                 "  -maxthreads <max>   : Max reads to thread a node [" + MAXTHREADREADS + "]\n" +
	        	                 "  -maxr5 <max>        : Max number of reads starting at a node [" + MAXR5 +"]\n" +
	        	                 "  -trim5 <t5>         : Bases to chop on 5' [" + TRIM5 + "]\n" +
	        	                 "  -trim3 <t3>         : Bases to chop on 3' [" + TRIM3 + "]\n" +
	        	                 "\n" +
	    		                 "removeTips : remove deadend tips\n" +
	    		                 "  -tiplen <len>       : Max tip length [" + -TIPLENGTH + "K]\n" +
	    		                 "\n" +
	    		                 "popBubbles : pop bubbles\n" +
	    		                 "  -bubblelen <len>    : Max Bubble length [" + -MAXBUBBLELEN + "K]\n" +
	    		                 "  -bubbleerrate <len> : Max Bubble Error Rate [" + BUBBLEEDITRATE + "]\n" +
	    		                 "\n" +
	    		                 "lowcov : cut low coverage nodes\n" +
	    		                 "  -lowcov <thresh>    : Cut coverage threshold [" + LOW_COV_THRESH + "]\n" +
	    		                 "  -lowcovlen <len>    : Cut length threshold [" + -MAX_LOW_COV_LEN + "K]\n" +
	    		                 "\n" +
	    	                     "repeats : thread short repeats\n" +
	    	                     "  -threads <min>      : Number threading reads [" + MIN_THREAD_WEIGHT + "]\n" +
	    	                     "  -maxthreads <max>   : Max reads to thread a node [" + MAXTHREADREADS + "]\n" +
	    		                 "  -record_all_threads : Record threads on non-branching nodes\n" +
	    	                     "\n" +
	    		                 "scaffolding : scaffold together contigs using mates\n" +
	    		                 "  -insertlen <len>    : Expected Insert size [required]\n" +
                                 "  -minuniquecov <min> : Min coverage to scaffold [required]\n" +
                                 "  -maxuniquecov <max> : Max coverage to scaffold [required]\n" +
                                 "  -minuniquelen <min> : Min contig len to scaffold [" + -MIN_CTG_LEN + "K]\n" +
                                 "  -maxfrontier <hops> : Max hops to explore [" + MAX_FRONTIER + "]\n" +
                                 "  -wiggle <bp>        : Insert wiggle length [" + MIN_WIGGLE + "]\n" +
                                 "\n" +
	    		                 "convertFasta : convert final assembly to fasta format\n" +
	    		                 "  -genome <len>       : Genome size for N50 computation\n" +
	    		                 "\n" +
	    		                 "General Options\n" +
	    		                 "===============\n" +
	    		                 "  -asm <asmdir>       : Hadoop Base directory for assembly [required]\n" +
	    		                 "  -reads <readsdir>   : Directory with reads [required]\n" + 
	    		                 "  -work <workdir>     : Local directory for output files [" + localBasePath + "]\n" +
	    		                 "  -slots <slots>      : Hadoop Slots to use [" + HADOOP_MAPPERS + "]\n" +
	        	                 "  -expert             : Show expert options\n");
	        	
	        	
	        	if (line.hasOption("expert"))
	        	{
	        	System.out.print("\n" +
	        			         "Conversion options\n" +
	        			         "================\n" +
	        			         "  -convert_fa <dir>      : Just convert assembly <dir> to fasta\n" +
	        			         "  -run_stats <dir>       : Just compute size stats of <dir>\n" +
	    		                 "    -genome <len>        : Genome size for N50 computation\n" +
	    		                 "  -print_fa <file>       : Convert localfile <file> to fasta\n" +
	    		                 "    -fasta_min_len <len> : Just convert contigs with this len [" + FASTA_MIN_LEN + "]\n"+
	    		                 "    -fasta_min_cov <cov> : Just convert contigs with this cov [" + FASTA_MIN_COV + "]\n"+
	    		                 "\n" +
	        			         "Hadoop Options\n" +
	        			         "==============\n" +
	    		                 "  -nodes <max>        : Max nodes in memory [" + HADOOP_LOCALNODES + "]\n" +
	    		                 "  -javaopts <opts>    : Hadoop Java Opts [" + HADOOP_JAVAOPTS + "]\n" +
	    		                 "  -timeout <usec>     : Hadoop task timeout [" + HADOOP_TIMEOUT + "]\n" +
	    		                 "  -validate           : Just validate options\n" +
	    		                 "  -go                 : Execute even when validating\n" +
	        			         "\n" +
	    		                 "Assembly Restart Options\n" +
	    		                 "========================\n" +
	    		                 "You must specific a start stage if you do not want to start at the very beginning\n" +
	    		                 "It is also recommended that you restart using a new assembly directory:\n" +
	    		                 "$ hadoop fs -cp /path/to/assembly/10-* /path/to/newassembly/\n" +
	    		                 "$ hadoop jar contrail.jar -asm /path/to/newassembly -start scafolding <...>\n\n" +
	    		                 "  -start <stage>      : Start stage: buildInitial|removeTips|popBubbles|lowcov|repeats|scaffolding|convertFasta\n" +
	    		                 "  -stop  <stage>      : Stop stage (default: convertFasta)\n" +
	    		                 "  -restart_initial               : Restart after build initial, before quickmerge\n" +
	    		                 "  -restart_compress <stage>      : Restart compress after this completed stage\n" +
	    		                 "  -restart_compress_remain <cnt> : Restart compress with these remaining\n" +
	    		                 "  -restart_tip <stage>           : Restart tips after this completed tips\n" +
	    		                 "  -restart_tip_remain <cnt>      : Restart tips with these remaining\n" +
	    		                 "  -restart_scaff_phase <phase>   : Restart at this phase\n" +
	    		                 "  -restart_scaff_stage <stage>   : Restart at this stage: edges, bundles, frontier, update, finalize, clean\n" + 
	    		                 "  -restart_scaff_frontier <hops> : Restart after this many hops\n"
	        					);
	        	}
	        	
	        	System.exit(0);
	        }
	        
	        if (line.hasOption("validate")) { validateonly = true; }
	        if (line.hasOption("go"))       { forcego = true; }
	        
	        if (line.hasOption("asm"))   { hadoopBasePath = line.getOptionValue("asm");  }
	        if (line.hasOption("reads")) { hadoopReadPath = line.getOptionValue("reads"); }
	        if (line.hasOption("work"))  { localBasePath  = line.getOptionValue("work"); }
	        
	        if (line.hasOption("slots"))    { HADOOP_MAPPERS  = Integer.parseInt(line.getOptionValue("slots")); HADOOP_REDUCERS = HADOOP_MAPPERS; }
	        if (line.hasOption("nodes"))    { HADOOP_LOCALNODES      = Integer.parseInt(line.getOptionValue("nodes")); }
	        if (line.hasOption("javaopts")) { HADOOP_JAVAOPTS = line.getOptionValue("javaopts"); }
	        if (line.hasOption("timeout"))  { HADOOP_TIMEOUT  = Long.parseLong(line.getOptionValue("timeout")); }
	        
	        if (line.hasOption("start")) { STARTSTAGE = line.getOptionValue("start"); }
	        if (line.hasOption("stop"))  { STOPSTAGE  = line.getOptionValue("stop");  }
	        
	        if (line.hasOption("restart_initial"))    { RESTART_USED = true; RESTART_INITIAL    = 1; }
	        if (line.hasOption("restart_tip"))        { RESTART_USED = true; RESTART_TIP        = Integer.parseInt(line.getOptionValue("restart_tip")); }
	        if (line.hasOption("restart_tip_remain")) { RESTART_USED = true; RESTART_TIP_REMAIN = Integer.parseInt(line.getOptionValue("restart_tip_remain")); }

	        if (line.hasOption("restart_compress"))        { RESTART_USED = true; RESTART_COMPRESS        = Integer.parseInt(line.getOptionValue("restart_compress")); }
	        if (line.hasOption("restart_compress_remain")) { RESTART_USED = true; RESTART_COMPRESS_REMAIN = Integer.parseInt(line.getOptionValue("restart_compress_remain")); }
	        
	        if (line.hasOption("restart_scaff_phase"))    { RESTART_USED = true; RESTART_SCAFF_PHASE    = Integer.parseInt(line.getOptionValue("restart_scaff_phase")); }
	        if (line.hasOption("restart_scaff_stage"))    { RESTART_USED = true; RESTART_SCAFF_STAGE    = line.getOptionValue("restart_scaff_stage"); }
	        if (line.hasOption("restart_scaff_frontier")) { RESTART_USED = true; RESTART_SCAFF_FRONTIER = Integer.parseInt(line.getOptionValue("restart_scaff_frontier")); }
	        
	        if (line.hasOption("filesuffix"))    { PREPROCESS_SUFFIX = 1; }
	        
	        if (line.hasOption("k"))     { K     = Long.parseLong(line.getOptionValue("k")); }
	        if (line.hasOption("maxr5")) { MAXR5 = Long.parseLong(line.getOptionValue("maxr5")); }
	        if (line.hasOption("trim3")) { TRIM3 = Long.parseLong(line.getOptionValue("trim3")); }
	        if (line.hasOption("trim5")) { TRIM5 = Long.parseLong(line.getOptionValue("trim5")); }
	        
	        if (line.hasOption("tiplen"))       { TIPLENGTH      = Long.parseLong(line.getOptionValue("tiplen")); }
	        if (line.hasOption("bubblelen"))    { MAXBUBBLELEN   = Long.parseLong(line.getOptionValue("bubblelen")); }
	        if (line.hasOption("bubbleerate"))  { BUBBLEEDITRATE = Long.parseLong(line.getOptionValue("bubbleerate")); }
	        
	        if (line.hasOption("lowcov"))       { LOW_COV_THRESH    = Float.parseFloat(line.getOptionValue("lowcov")); }
	        if (line.hasOption("lowcovlen"))    { MAX_LOW_COV_LEN   = Long.parseLong(line.getOptionValue("lowcovlen")); }

	        if (line.hasOption("threads"))             { MIN_THREAD_WEIGHT = Long.parseLong(line.getOptionValue("threads")); }
	        if (line.hasOption("maxthreads"))          { MAXTHREADREADS    = Long.parseLong(line.getOptionValue("maxthreads")); }
	        if (line.hasOption("record_all_threads"))  { RECORD_ALL_THREADS = 1; }
	        
	        if (line.hasOption("insertlen"))    { INSERT_LEN     = Long.parseLong(line.getOptionValue("insertlen")); }
	        if (line.hasOption("wiggle"))       { MIN_WIGGLE     = Long.parseLong(line.getOptionValue("wiggle")); }
	        if (line.hasOption("minuniquelen")) { MIN_CTG_LEN    = Long.parseLong(line.getOptionValue("minuniquelen")); }
	        if (line.hasOption("minuniquecov")) { MIN_UNIQUE_COV = Float.parseFloat(line.getOptionValue("minuniquecov")); }
	        if (line.hasOption("maxuniquecov")) { MAX_UNIQUE_COV = Float.parseFloat(line.getOptionValue("maxuniquecov")); }
	        if (line.hasOption("maxfrontier"))  { MAX_FRONTIER = Long.parseLong(line.getOptionValue("maxfrontier")); }
	    
	        if (line.hasOption("genome"))       { N50_TARGET = Long.parseLong(line.getOptionValue("genome")); }
	        if (line.hasOption("run_stats"))    { RUN_STATS = line.getOptionValue("run_stats"); }
	        
	        if (line.hasOption("convert_fa"))    { CONVERT_FA    = line.getOptionValue("convert_fa"); }
	        if (line.hasOption("print_fa"))      { PRINT_FA      = line.getOptionValue("print_fa"); }
	        if (line.hasOption("fasta_min_len")) { FASTA_MIN_LEN = Long.parseLong(line.getOptionValue("fasta_min_len")); }
	        if (line.hasOption("fasta_min_cov")) { FASTA_MIN_COV = Float.parseFloat(line.getOptionValue("fasta_min_cov")); }
	    }
	    catch( ParseException exp ) 
	    {
	        // oops, something went wrong
	        System.err.println( "Parsing failed.  Reason: " + exp.getMessage() );
	        System.exit(1);
	    }
	}
}
