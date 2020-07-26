package com.aexp.cs.bulletproofing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
/***
 * 
 * @author Ravi.Mandholia
 *
 */
public class DelimitedParserDriver {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		// add resources
		//conf.addResource(new Path(args[2]));
		String queue_name =args[2];
		String delimiter=args[3];
		String primaryKeys=args[4];
		
		//String queue_name = conf.get("queue_name");

		// queue_name from config file
		conf.set("mapred.job.queue.name", queue_name);
		conf.set("output_path", args[1]);
		conf.set("delimiter",delimiter);
		conf.set("primaryKeys",primaryKeys);
		
		Job job = new Job(conf, "Delimited Unique Records");
		job.setJarByClass(DelimitedParserDriver.class);
		job.setMapperClass(DelimitedParserMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setReducerClass(DelimitedParserReducer.class);
		MultipleOutputs.addNamedOutput(job, "duplicate", TextOutputFormat.class, Text.class, Text.class);

		//Setting input and output path
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		//custom input format is created to read xml file
		job.setInputFormatClass(TextInputFormat.class);

		//default outputformat
		job.setOutputFormatClass(TextOutputFormat.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
