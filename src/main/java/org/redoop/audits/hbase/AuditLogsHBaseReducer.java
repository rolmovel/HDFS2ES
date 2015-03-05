package org.redoop.audits.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuditLogsHBaseReducer extends 
			TableReducer<Text, Text,ImmutableBytesWritable> {
	
	private static Logger log = LoggerFactory.getLogger(AuditLogsHBaseReducer.class);

	private static final String FAMILY = "log";
	private static final String COL_FROM = "from";
	private static final String COL_IP = "ip";
	private static final String COL_HOSTNAME = "hostname";
	private static final String COL_USERNAME = "username";
    
	public void reduce(Text _key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// Process values, unnecessary in this case because we have only one value for each key.
		// This is just an example to show how to write to HBase in the Reducer Class. 
		String line="";
		for (Text val : values) {
			line+=val.toString();
		} 
		
		// Put to HBase       
        String fields[] = line.split(" ");
        
        Put put = new Put(Bytes.toBytes(_key.toString()));
        
        String from = fields[0];
        put.add(Bytes.toBytes(FAMILY),Bytes.toBytes(COL_FROM),Bytes.toBytes(from));
        
		String username = fields[1];
		put.add(Bytes.toBytes(FAMILY),Bytes.toBytes(COL_USERNAME),Bytes.toBytes(username));

		String ip = fields[2];
		put.add(Bytes.toBytes(FAMILY),Bytes.toBytes(COL_IP),Bytes.toBytes(ip));

		String hostname = fields[3];
		put.add(Bytes.toBytes(FAMILY),Bytes.toBytes(COL_HOSTNAME),Bytes.toBytes(hostname));

		context.write(null, put);
		
	}

}
