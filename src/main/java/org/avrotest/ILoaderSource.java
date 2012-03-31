package org.avrotest;

// interface for loader.
public interface ILoaderSource {
	
	// copy data to local FS.
	// params 1) src - file source
	// params 2) dstLocalDir - Local Directory to which data will be pasted.
	void copyDataToLocal(String src, String dstLocalDir);
	//load data to HDFS
	void loadDataToHdfs(String srcPath, String dstHdfsDir, String schemaFile);
	
}
