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
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.AvroValue;
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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*This file converts a part file of kmer count which is in Avro format into Non Avro
 * <kmer, count> key value pair. This file will then be used for cutoff calculation
 */

public class convertPartFileToNonAvro {
	  
	/* Simply reads the file from HDFS and gives it to the reducer*/
	    public static class copyPartMapper extends AvroMapper<Pair<Utf8, Long>, Pair<Utf8, Long>>        
      {
              

              @Override
              public void map(Pair<Utf8, Long> count_record, 
                  AvroCollector<Pair<Utf8, Long>> output, Reporter reporter)
                                              throws IOException {

                  	output.collect(count_record);
                
              }
      }
     
	    /*The output schema is the pair schema of <kmer, count>*/
	    public static class copyPartReducer extends MapReduceBase implements 
	    Reducer <AvroKey<Utf8>, AvroValue<Long>, Text, LongWritable > {
	    	public void reduce(AvroKey<Utf8> kmer, Iterator<AvroValue<Long>> counts, 
	    						OutputCollector<Text, LongWritable> collector,
	    						Reporter reporter) throws IOException {
	     	
	    			while(counts.hasNext())
	    			{
	    				AvroValue<Long> count = counts.next();
	    				//System.out.println("Kmer "+kmer.toString()+ "Count "+ count.datum().toString());
	    				collector.collect(new Text(kmer.datum().toString()), new LongWritable(count.datum()));
	    			}
	     	}

			
	     } 


protected int run(String inputPath, String outputPath) throws Exception {  
 
  
  JobConf conf = new JobConf(kmerCounter.class);
  conf.setJobName("testing ");

  FileInputFormat.addInputPath(conf, new Path(inputPath));
  FileOutputFormat.setOutputPath(conf, new Path(outputPath));
  //conf.setR(Text.class);
  //conf.setMapOutputValueClass(LongWritable.class);
  AvroJob.setInputSchema(conf, new Pair<Utf8,Long>(new Utf8(""), 0L).getSchema());
  AvroJob.setMapOutputSchema(conf, new Pair<Utf8,Long>(new Utf8(""), 0L).getSchema());

  //AvroJob.setOutputSchema(conf, );
  
  AvroJob.setMapperClass(conf, copyPartMapper.class);
  conf.setReducerClass(copyPartReducer.class);
  conf.setOutputKeyClass(Text.class);
  conf.setOutputValueClass(LongWritable.class);
 // conf.setNumReduceTasks(0);

     // Delete the output directory if it exists already
  Path out_path = new Path(outputPath);
  if (FileSystem.get(conf).exists(out_path)) {
    FileSystem.get(conf).delete(out_path, true);  
  }
  
  long starttime = System.currentTimeMillis();            
  JobClient.runJob(conf);
  long endtime = System.currentTimeMillis();
  float diff = (float) (((float) (endtime - starttime)) / 1000.0);
  System.out.println("Runtime: " + diff + " s");
  return 0;
      
}
   
}


