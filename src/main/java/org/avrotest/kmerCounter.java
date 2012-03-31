package org.avrotest;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.avrotest.fastqrecord;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapred.Pair;
import org.apache.avro.reflect.ReflectData;
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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class kmerCounter
{       
        
        public static class KmerCounterMapper extends AvroMapper<fastqrecord, fastqrecord>        
        {
                private static int K = 0;

                public void configure(JobConf job) 
                {
                        //K = Integer.parseInt(job.get("K"));
                        
                }
          
 
             
                @Override
                public void map(fastqrecord compressed_read, 
                    AvroCollector<fastqrecord> output, Reporter reporter)
                                                throws IOException {

                	System.out.print("Read = "+compressed_read.read);
                   	output.collect(compressed_read);
        }
        }
       

  
  protected int run(String inputPath, String outputPath, long K) throws Exception {  
   
    
    JobConf conf = new JobConf(kmerCounter.class);
    conf.setJobName("testing ");

    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    fastqrecord read = new fastqrecord();
    AvroJob.setInputSchema(conf, read.getSchema());
    AvroJob.setMapOutputSchema(conf, read.getSchema());
    AvroJob.setMapperClass(conf, KmerCounterMapper.class);
    conf.setNumReduceTasks(0);
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