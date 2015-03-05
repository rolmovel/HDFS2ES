package org.redoop.audits.hdfs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AuditLogsMapper extends Mapper<LongWritable, Text, Text, Text> {

	private static final String ADD_GROUP = "ADD_GROUP";
	private static final String ADD_USER = "ADD_USER";
	
	private static final String CRED_ACQ = "CRED_ACQ"; 
	private static final String CRED_DISP = "CRED_DISP"; 
	private static final String CRED_REFR = "CRED_REFR"; 
	
	private static final String CRYPTO_KEY_USER = "CRYPTO_KEY_USER"; 
	private static final String CRYPTO_SESSION = "CRYPTO_SESSION";
	
	private static final String DEL_GROUP = "DEL_GROUP";
	private static final String DEL_USER = "DEL_USER";
	
	private static final String LOGIN = "LOGIN"; 
	
	private static final String USER_ACCT = "USER_ACCT"; 
	private static final String USER_AUTH = "USER_AUTH"; 
	private static final String USER_END = "USER_END"; 
	private static final String USER_LOGIN = "USER_LOGIN"; 
	private static final String USER_LOGOUT = "USER_LOGOUT"; 
	private static final String USER_START = "USER_START"; 
	
	public void map(LongWritable ikey, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		
		String fields[] = line.split(" ");
		String type = fields[1].split("=")[1];
		
		for (String field : fields){
			if(field.contains("ses=")){
				context.write(new Text(field.split("=")[1]), new Text(type));
			}
		}
		
	}

}
