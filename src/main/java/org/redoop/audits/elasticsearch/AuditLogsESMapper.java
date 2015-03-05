package org.redoop.audits.elasticsearch;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuditLogsESMapper extends Mapper <LongWritable, Text, LongWritable, MapWritable> {
	private static Logger log = LoggerFactory.getLogger(AuditLogsESMapper.class);

	private static String transformDate(String date, String originPtt, String finalPtt) {
		try {
		SimpleDateFormat sdf1 = new SimpleDateFormat(originPtt);
		Date date1 = sdf1.parse(date);
		
		SimpleDateFormat sdf2 = new SimpleDateFormat(finalPtt);
		
		return sdf2.format(date1);
		} catch (Exception e) {
			return "";
		}
	}
	
	@Override
	 protected void map(LongWritable key, Text value, Context context)
	        throws IOException, InterruptedException {
		
		try{

			log.info("Starting Map Job");
			String line = value.toString();
	        String field[] = line.split(",");
			//String type = fields[1].split("=")[1];
			
			// create the MapWritable object
			MapWritable doc = new MapWritable();
			   
				
				doc.put(new Text("transaction_date"), new Text(transformDate(field[0], "d/M/yy H:mm", "yyyy-MM-dd'T'HH:mm:ss.SSSZ")));
				doc.put(new Text("product"), new Text(field[1]));
				doc.put(new Text("price"), new Text(field[2]));
				doc.put(new Text("payment_type"), new Text(field[3]));
				doc.put(new Text("name"), new Text(field[4]));
				doc.put(new Text("city"), new Text(field[5]));
				doc.put(new Text("state"), new Text(field[6]));
				doc.put(new Text("country"), new Text(field[7]));
				doc.put(new Text("account_created"), new Text(transformDate(field[8], "d/M/yy H:mm", "yyyy-MM-dd'T'HH:mm:ss.SSSZ")));
				doc.put(new Text("last_login"), new Text(field[9]));
				doc.put(new Text("latitude"), new Text(field[10]));
				doc.put(new Text("longitude"), new Text(field[11]));
			
				context.write(key, doc);
		


/*
			log.info("Starting Map Job");
			String line = value.toString();
	        //String fields[] = line.split(" ");
			//String type = fields[1].split("=")[1];
			
			// create the MapWritable object
			MapWritable doc = new MapWritable();
			   
			doc.put(new Text("line"), new Text(line));
			
			context.write(key, doc);
			
			//PARSE ONLY SUCCESSFUL USER_LOGIN EVENTS
			if (type.equalsIgnoreCase("USER_LOGIN") && line.contains("res=success")){
				
				String node = fields[0].split("=")[1];
				doc.put(new Text("node"), new Text(node));
				
				String from = fields[11].split("=")[1];
				doc.put(new Text("from"), new Text(from));
			
				String username = fields[15].split("=")[1];
				doc.put(new Text("username"), new Text(username));
				
				String ip = fields[12].split("=")[1];
				doc.put(new Text("ip"), new Text(ip));
				
				String timestamp = fields[2].substring(10).split(":")[0].substring(0, fields[0].length()-4);
				doc.put(new Text("timestamp"), new Text(timestamp));
				
				context.write(key, doc);
			}
			*/
		
		}catch (Exception e) {
	        e.printStackTrace();
	    }

	 }
}
