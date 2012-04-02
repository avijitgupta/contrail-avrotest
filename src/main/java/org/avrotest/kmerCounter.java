package org.avrotest;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.avrotest.fastqrecord;
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




public class kmerCounter
{       
        /*The input schema to this mapper is the normal fastq schema
         * id, read, qvalue
         */
	    public static class KmerCounterMapper extends AvroMapper<fastqrecord, Pair<Utf8, Long>>        
        {
                private static long K = 0;

                public void configure(JobConf job) 
                {
                       K= job.getLong("K", 0);
                        
                }

                @Override
                public void map(fastqrecord compressed_read, 
                    AvroCollector<Pair<Utf8, Long>> output, Reporter reporter)
                                                throws IOException {

                	String id = compressed_read.id.toString();
                	String seq= compressed_read.read.toString();
                  

                	/* We convert every kmer to its canonical for the kmer counting phase so that
            		 * canonical kmers dont appear at different places
            		 */
                	for (int i=0; i<= seq.length() - K; i++)
                	{
                		String kmer = seq.substring(i,(int)(i+K));
                		String kmerCanonical = Node.canonicalseq(kmer);
                		//
                		output.collect(new Pair<Utf8,Long>(new Utf8(kmerCanonical),1L));
                    }
                }
        }
       
	    public static class KmerCounterReducer extends AvroReducer<Utf8, Long, Pair<Utf8, Long> > {
	    	@Override
	     	public void reduce(Utf8 kmer, Iterable<Long> counts, AvroCollector<Pair<Utf8,Long>> collector,
	     	Reporter reporter) throws IOException {
	     	long sum = 0;
	     	for (long count : counts)
	     	sum += count;
	     	collector.collect(new Pair<Utf8,Long>(kmer, sum));
	     	}
	     } 

  
  protected int run(String inputPath, String outputPath, long K) throws Exception {  
   
    
    JobConf conf = new JobConf(kmerCounter.class);
    conf.setJobName("Kmer Counter ");

    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));
    conf.setLong("K", K);
    fastqrecord read = new fastqrecord();
    AvroJob.setInputSchema(conf, read.getSchema());
    AvroJob.setOutputSchema(conf, new Pair<Utf8,Long>(new Utf8(""), 0L).getSchema());
    AvroJob.setMapperClass(conf, KmerCounterMapper.class);
    AvroJob.setReducerClass(conf, KmerCounterReducer.class);
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