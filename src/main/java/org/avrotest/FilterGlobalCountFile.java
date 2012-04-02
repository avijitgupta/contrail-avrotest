package org.avrotest;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapred.Pair;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.util.Utf8;
import org.apache.avro.Schema;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.util.*;
/* This class prunes the count file according to the cutoff obtained
 * The mapper reads in Avro records and emits out avro records if the value
 * of count is greater than cutoff. This significantly reduces file size  
 */
public class FilterGlobalCountFile {


   	public static class FilterMapper 
       extends AvroMapper<Pair<Utf8, Long>, Pair<Utf8, Long>> {	
		private int cutOff;

    public void configure(JobConf job) 
    {
    	cutOff = job.getInt("cutoff",0);
    }

	//incoming key,value pairs - (kmer, frequency)
    public void map(Pair<Utf8, Long> count_record, 
            AvroCollector<Pair<Utf8, Long>> output, Reporter reporter) throws IOException {
    
    	long freq_kmer = count_record.value().longValue();
    	    
    	if(freq_kmer>= cutOff) {
    		
    		output.collect(count_record); 
    	}
    }
	}
    
    
    public static void FilterKmers(String inputPath, String outputPath, long cutoff) throws Exception {

    	JobConf conf = new JobConf(kmerCounter.class);
        conf.setJobName("Filter Kmer Counts ");

        FileInputFormat.addInputPath(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));
        conf.setLong("cutoff", cutoff);
        AvroJob.setInputSchema(conf, new Pair<Utf8,Long>(new Utf8(""), 0L).getSchema());
        AvroJob.setOutputSchema(conf, new Pair<Utf8,Long>(new Utf8(""), 0L).getSchema());
        AvroJob.setMapperClass(conf, FilterMapper.class);
        conf.setNumReduceTasks(0);
      
        Path out_path = new Path(outputPath);
        if (FileSystem.get(conf).exists(out_path)) {
          FileSystem.get(conf).delete(out_path, true);  
        }
        
        long starttime = System.currentTimeMillis();            
        JobClient.runJob(conf);
        long endtime = System.currentTimeMillis();
        float diff = (float) (((float) (endtime - starttime)) / 1000.0);
        System.out.println("Runtime: " + diff + " s");
        return ;
    	  
    }



}

