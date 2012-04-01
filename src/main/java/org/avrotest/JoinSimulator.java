package org.avrotest;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroJob; 
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapred.lib.NLineInputFormat;

public class JoinSimulator {
	public static final Schema OUT_SCHEMA = ReflectData.get().getSchema(FqRecord.class);

	
	  public static class FastqPreprocessorMapper extends MapReduceBase  implements Mapper<LongWritable, Text, AvroWrapper<joinedfqrecord>, NullWritable> 
	  {
	    private int idx = 0;

	    private String name = null;
	    private String fqid = null;
	    private String fqread = null; 
	    private String filename = null;
	    private String fqqvalue = null;

	    private int mate_id = 0x0;

	    /**
	     *  The alphabet for encoding the sequences.
	     */
	  //  private Alphabet alphabet;

	    private String counter = "pair_unknown";


	    // initial size for the buffer used to encode the dna sequence
	    private int START_CAPACITY = 200;

	    private joinedfqrecord read = new joinedfqrecord();
	    private AvroWrapper<joinedfqrecord> out_wrapper = new AvroWrapper<joinedfqrecord>(read);

	  //  private ByteReplaceAll replacer = null; 

	    // The byte value to replace multi-byte characters with
	    // this an underscore.
	    public final byte MULTIBYTE_REPLACE_VALUE = 0x5f;

	    // The sequence.
	  //  private Sequence sequence;
	    
	    // An array which can be used to tell if a UTF8 value
	    // is whitespace
	    private boolean[] utf8_whitespace;
	    
	    // Store the utf8 byte values of various characters
	    private byte utf8_at;
	    private byte utf8_space;
	    
	    public void configure(JobConf job) 
	    {
	    
	    }

	    public void map(LongWritable lineid, Text line, OutputCollector<AvroWrapper<joinedfqrecord>, NullWritable> output, Reporter reporter) throws IOException 
	    {
	  	//  System.out.print("In Mapper");

	      if (idx == 0) 
	      {    
	    	  fqid = line.toString();
	      }
	      else if (idx == 1) {    
	    	
	    	  fqread = line.toString();
	      }
	      else if (idx == 2) { 
	      }
	      else if (idx == 3)
	      {               
	    	 fqqvalue = line.toString();
	    	 read.id1 = fqid;
	    	 read.qvalue1 = fqqvalue;
	    	 read.read1 = fqread;
	    	 read.id2 = fqid;
	    	 read.qvalue2 = fqqvalue;
	    	 read.read2 = fqread;
	    	                                              
	        output.collect(out_wrapper, NullWritable.get());
	
	      }

	      idx = (idx + 1) % 4;
	            }

	    public void close() throws IOException
	    {
	      if (idx != 0)
	      {
	    //    throw new IOException("ERROR: closing with idx = " + idx + " in " + filename);
	      }
	    }

		
	  }


	  // Run Tool
	  ///////////////////////////////////////////////////////////////////////////           
	  public RunningJob run(String inputPath, String outputPath) throws Exception
	  { 
	 
	    JobConf conf = new JobConf(FastQtoAvro.class);
	    conf.setJobName("Conversion from text to Avro" + inputPath);

	    FileInputFormat.addInputPath(conf, new Path(inputPath));
	    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

	    conf.setInputFormat(TextInputFormat.class);

	    conf.setMapOutputKeyClass(Text.class);
	    conf.setMapOutputValueClass(Text.class);

	    conf.setMapperClass(FastqPreprocessorMapper.class);
	    conf.setNumReduceTasks(0);

	    conf.setInputFormat(NLineInputFormat.class);
	    conf.setInt("mapred.line.input.format.linespermap", 2000000); // must be a multiple of 4

	    // TODO(jlewi): use setoutput codec to set the compression codec. 
	    joinedfqrecord read = new joinedfqrecord();
	    
	    AvroJob.setOutputSchema(conf,read.getSchema());
	    

	    //delete the output directory if it exists already
	    FileSystem.get(conf).delete(new Path(outputPath), true);

	    
	    long start_time = System.currentTimeMillis();    
	    RunningJob result = JobClient.runJob(conf);
	    long end_time = System.currentTimeMillis();    
	    double nseconds = (end_time - start_time) / 1000.0;
	    System.out.println("Job took: " + nseconds + " seconds");
	    
	    return result;
	  }
}



