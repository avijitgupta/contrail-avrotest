/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package org.avrotest;

@SuppressWarnings("all")
/** Data structures for contrail */
public interface ContrailProtocol {
  public static final org.apache.avro.Protocol PROTOCOL = org.apache.avro.Protocol.parse("{\"protocol\":\"ContrailProtocol\",\"namespace\":\"org.avrotest\",\"doc\":\"Data structures for contrail\",\"types\":[{\"type\":\"record\",\"name\":\"fastqrecord\",\"doc\":\"A Fastq Read\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"read\",\"type\":\"string\"},{\"name\":\"qvalue\",\"type\":\"string\"}]},{\"type\":\"record\",\"name\":\"kcount\",\"doc\":\"Kmer count Structure\",\"fields\":[{\"name\":\"kmer\",\"type\":\"string\"},{\"name\":\"count\",\"type\":\"long\"}]}],\"messages\":{}}");

  @SuppressWarnings("all")
  /** Data structures for contrail */
  public interface Callback extends ContrailProtocol {
    public static final org.apache.avro.Protocol PROTOCOL = org.avrotest.ContrailProtocol.PROTOCOL;
  }
}