package org.redoop.audits.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuditLogsHBaseMapper extends
		Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
	// For reducers
	// Mapper<LongWritable, Text, Text,Text> {
	
	private static Logger log = LoggerFactory.getLogger(AuditLogsHBaseMapper.class);

	
	private static final String FAMILY = "log";
	private static final String COL_FROM = "from";
	private static final String COL_IP = "ip";
	private static final String COL_HOSTNAME = "hostname";
	private static final String COL_USERNAME = "username";
    
    /**
     * Maps the input.
     *
     * @param offset The current offset into the input file.
     * @param line The current line of the file.
     * @param context The task context.
     * @throws IOException When mapping the input fails.
     */
    @Override
    public void map(LongWritable offset, Text value, Context context) 
    throws IOException {
      try {
    	log.info("Starting Map Job");
        String line = value.toString();
        String fields[] = line.split(" ");
		String type = fields[1].split("=")[1];
		
		//PARSE ONLY SUCCESSFUL USER_LOGIN EVENTS
		if (type.equalsIgnoreCase("USER_LOGIN") && line.contains("res=success")){
			// Key generation part
			String node = fields[0].split("=")[1];
			String timestamp = fields[2].substring(10).split(":")[0].substring(0, fields[0].length()-4);
			String sKey = node + "|" + type + "|" + timestamp;			
			
			// Put part (For Map Only Jobs)
			// To use reducers comment the lines below.
			byte[] rowkey = Bytes.toBytes(sKey); 
			Put put = new Put(rowkey);
			
			// Value part (For Map Only Jobs). 
			// To use reducers comment put lines.
			String from = fields[11].split("=")[1];
			put.add(Bytes.toBytes(FAMILY),Bytes.toBytes(COL_FROM),Bytes.toBytes(from));
			
			String username = fields[15].split("=")[1];
			put.add(Bytes.toBytes(FAMILY),Bytes.toBytes(COL_USERNAME),Bytes.toBytes(username));
			
			String ip = fields[12].split("=")[1];
			put.add(Bytes.toBytes(FAMILY),Bytes.toBytes(COL_IP),Bytes.toBytes(ip));
			
			String hostname = node;
			put.add(Bytes.toBytes(FAMILY),Bytes.toBytes(COL_HOSTNAME),Bytes.toBytes(hostname));
			
			//Write to Context (HBase)
			context.write(null, put);
			
			/* Write to Context (Reduce)
			String v =  from+" "+username+" "+ip+" "+hostname;
			context.write(new Text(sKey),new Text(v));
			*/ 
			
		}
		
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

}
