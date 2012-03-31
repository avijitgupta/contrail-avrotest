package org.avrotest;


// This is a schema class and is used in AvroReader. Avro uses this class to generate
//a schema.
public class FqRecord {

	String read;
	String id;
	String qvalue;
	
	public FqRecord(){
		
	}
	
	public FqRecord(String id, String read, String qvalue ){
		
		this.id = id;
		this.read = read;
		this.qvalue = qvalue;	
	}
	
    public String toString() {
        return String.format("Record: %s %s %s", read, id, qvalue);
    }
}
