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


/**
 * Map reduce job to encode FastQ files in sequence files using AVRO.
 * DNA sequences are encoded as bytes arrays. The DNA sequence is packed into an array
 * of bytes using 3 bits per letter.  
 * 
 * We encode the data as byte arrays and try to avoid convertng it to a String because
 * toString() is expensive.
 *
 */

public class FastQtoAvro 
{       
  /**
   * Mapper.
   * 
   * @author jlewi
   *
   */
	

	
  public static class FastqPreprocessorMapper extends MapReduceBase  implements Mapper<LongWritable, Text, AvroWrapper<fastqrecord>, NullWritable> 
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

    private fastqrecord read = new fastqrecord();
    private AvroWrapper<fastqrecord> out_wrapper = new AvroWrapper<fastqrecord>(read);

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

    public void map(LongWritable lineid, Text line, OutputCollector<AvroWrapper<fastqrecord>, NullWritable> output, Reporter reporter) throws IOException 
    {
  	//  System.out.print("In Mapper");

      if (idx == 0) 
      { 
    	
    	  fqid = line.toString();
      }
      else if (idx == 1) {    
    	//  System.out.print("In line 2");
    	  
      /*  byte[] raw_bytes = line.getBytes();
        // TODO(jeremy@lewi.us): We should really only be checking the bytes upto line.getLength()
        if (ByteUtil.hasMultiByteChars(raw_bytes)){
          throw new RuntimeException("DNA sequence contained illegal characters. Sequence is: " + line.toString());
        }

        sequence.readUTF8(raw_bytes, line.getLength());
        int num_bytes =  (int)Math.ceil((alphabet.bitsPerLetter() * sequence.size())/ 8.0);
        
        read.dna = ByteBuffer.wrap(sequence.toPackedBytes(), 0, num_bytes);      */  
    	  fqread = line.toString();
      }
      else if (idx == 2) { 
      }
      else if (idx == 3)
      {               
    	 fqqvalue = line.toString();
    	 int ind = fqid.lastIndexOf('/');
     	 if (ind==-1){}//fastq not in proper format for mate pair reading
     	 fqid = fqid.substring(0,ind);
    	 read.id = fqid;
    	 read.qvalue = fqqvalue;
    	 read.read = fqread;
    	  // System.out.print("In line 4");
     /*   read.id = name;*/                                                 
        output.collect(out_wrapper, NullWritable.get());
/*
        reporter.incrCounter("Contrail", "preprocessed_reads", 1);
        reporter.incrCounter("Contrail", counter, 1);*/
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
	fastqrecord read = new fastqrecord();
	Schema OUT_SCHEMA = read.getSchema();

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
    AvroJob.setOutputSchema(conf,OUT_SCHEMA);

    //delete the output directory if it exists already
    Path out_path = new Path(outputPath);
    if (FileSystem.get(conf).exists(out_path)) {
      FileSystem.get(conf).delete(out_path, true);  
    }

    
    long start_time = System.currentTimeMillis();    
    RunningJob result = JobClient.runJob(conf);
    long end_time = System.currentTimeMillis();    
    double nseconds = (end_time - start_time) / 1000.0;
    System.out.println("Job took: " + nseconds + " seconds");
    
    return result;
  }


  
}