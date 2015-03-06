package org.redoop.audits;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InvalidInputException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.elasticsearch.hadoop.mr.EsOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuditLogs {
	
	private static Logger log = LoggerFactory.getLogger(AuditLogs.class);
	
	private static final String HDFS = "hdfs";
	private static final String HBASE = "hbase";
	private static final String ELASTICSEARCH = "es";
	
	private static final String TABLE = "test";

	public static void main(String[] args) throws Exception {
		for (int i=0;i<args.length;i++) {
			System.out.println("Parametro " + i + " = " + args[i]); 
		}
		if (args.length != 3) {
			System.err.println("Usage: AuditLogs <sink type [hbase,hdfs,es]> <input path> <output path>");
			System.exit(-1);
			log.error("Usage: AuditLogs <sink type [hbase,hdfs,es]> <input path> <output path>");
		}
		
		//SET JOB SINK TYPE
		String sink = args[0];
		
		//SET JOB CONFIG AND CLASSES
		Job job;
		switch (sink){
			case (HDFS):
				job = runHDFSJob(sink, args[1], args[2]);
				System.exit(job.waitForCompletion(true) ? 0 : 1);
			case (HBASE):
				job = runHBASEJob(sink, args[1]);	
				System.exit(job.waitForCompletion(true) ? 0 : 1);
			case (ELASTICSEARCH):
				job = runElasticSearchJob(sink, args[1]);
				System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
	}
	
	private static Job runHBASEJob(String sink, String input) throws Exception {
		log.info("Configuring HBase Job");
		Configuration conf =  HBaseConfiguration.create();
		Job job = Job.getInstance(conf, "Audit Logs Eclipse (Logins) "+ sink + "Table: "+TABLE);
	    job.setJarByClass(org.redoop.audits.AuditLogs.class);
	    job.setMapperClass(org.redoop.audits.hbase.AuditLogsHBaseMapper.class);
	    
	    // Use a Map only Job (comment lines below for reducers confs)
	    job.setOutputFormatClass(TableOutputFormat.class);
	    job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, TABLE);
	    job.setOutputKeyClass(ImmutableBytesWritable.class);
	    job.setOutputValueClass(Put.class);
	    TableMapReduceUtil.addDependencyJars(job);
		TableMapReduceUtil.addDependencyJars(job.getConfiguration());
	    
	    /* Use a Reducer Job (comment above lines for map only confs) 
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
    	TableMapReduceUtil.initTableReducerJob(TABLE,org.redoop.audits.hbase.AuditLogsHBaseReducer.class, job);
	    job.setReducerClass(org.redoop.audits.hbase.AuditLogsHBaseReducer.class);
	    */
	    
	    //  Testing performance and tuning 
 		//  The right number of reduces seems to be 
 		//  0.95 or 1.75 * (nodes * mapred.tasktracker.reduce.tasks.maximum)
		//  0 ONLY MAP (write directly to HBase for example)
		// >0 USE REDUCERS
 		job.setNumReduceTasks(0); 
 		
 		// SET INPUT PATHS
 		FileSystem fs = FileSystem.get(conf);
		List<IOException> rslt = new ArrayList<IOException>();
		FileStatus[] inputs = fs.globStatus(new Path(input));
		if(inputs.length > 0) {
		      for (FileStatus onePath: inputs) {
		    	  FileInputFormat.addInputPath(job, onePath.getPath());
		      }
		} else {
		      rslt.add(new IOException("Input source " + input + " does not exist."));
		}
		if (!rslt.isEmpty()) {
		    throw new InvalidInputException(rslt);
		}
		return job;
	}

	public static Job runHDFSJob(String sink, String input, String output) throws Exception{
		log.info("Configuring HDFS Job");
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Audit Logs Eclipse (Logins) "+ sink);
		job.setJarByClass(org.redoop.audits.AuditLogs.class);
		job.setMapperClass(org.redoop.audits.hdfs.AuditLogsMapper.class);
		job.setReducerClass(org.redoop.audits.hdfs.AuditLogsReducer.class);
		
		// Specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// SET OUTPUT PATH
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		// FOR TESTING 
		// The right number of reduces seems to be 
		// 0.95 or 1.75 * (nodes * mapred.tasktracker.reduce.tasks.maximum)
		job.setNumReduceTasks(10);
		
		// DELETE OUTPUT DIRECTORY (FOR DEVELOPMENT)
		FileSystem fs = FileSystem.get(conf);
		if(sink.equalsIgnoreCase(HDFS)){
			fs.delete(new Path(output), true);
		}
		
		// SET INPUT PATHS
		List<IOException> rslt = new ArrayList<IOException>();
		FileStatus[] inputs = fs.globStatus(new Path(input));
		if(inputs.length > 0) {
		      for (FileStatus onePath: inputs) {
		    	  FileInputFormat.addInputPath(job, onePath.getPath());
		      }
		} else {
		      rslt.add(new IOException("Input source " + input + " does not exist."));
		}
		if (!rslt.isEmpty()) {
		    throw new InvalidInputException(rslt);
		}
		return job;
		
	}
	
	private static Job runElasticSearchJob(String sink, String input)throws Exception{
		log.info("Configuring ElasticSearch Job");
		
		Configuration conf = new Configuration();
		//conf.setBoolean("mapreduce.map.speculative", false);    
		//conf.setBoolean("mapreduce.reduce.speculative", false);
		conf.set("es.nodes", "22.0.6.34:9200");
		conf.set("es.resource", "negocio/{delivery}");
		
		Job job = Job.getInstance(conf, "Audit Logs Eclipse (Logins) ElasticSearch");
		job.setJarByClass(org.redoop.audits.AuditLogs.class);
	    job.setMapperClass(org.redoop.audits.elasticsearch.AuditLogsESMapper.class);
	    job.setSpeculativeExecution(false);
	    
	    // Specify output types
		job.setOutputFormatClass(EsOutputFormat.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(MapWritable.class);
		
		//  Testing performance and tuning 
 		//  The right number of reduces seems to be 
 		//  0.95 or 1.75 * (nodes * mapred.tasktracker.reduce.tasks.maximum)
		//  0 ONLY MAP (write directly to HBase for example)
		// >0 USE REDUCERS
 		job.setNumReduceTasks(0);
 		
 		// SET INPUT PATHS
 		FileSystem fs = FileSystem.get(conf);
		List<IOException> rslt = new ArrayList<IOException>();
		FileStatus[] inputs = fs.globStatus(new Path(input));
		if(inputs.length > 0) {
		      for (FileStatus onePath: inputs) {
		    	  FileInputFormat.addInputPath(job, onePath.getPath());
		      }
		} else {
		      rslt.add(new IOException("Input source " + input + " does not exist."));
		}
		if (!rslt.isEmpty()) {
		    throw new InvalidInputException(rslt);
		}

		return job;
	}
}
