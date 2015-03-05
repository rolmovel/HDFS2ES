package org.redoop.audits.hdfs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AuditLogsReducer extends Reducer<Text, Text, Text, Text> {
	
	
	
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// process values
		String events = "";
		for (Text val : values) {
			events = events + " " + val.toString();

		}
		context.write(key, new Text(events));

		
	}

}
