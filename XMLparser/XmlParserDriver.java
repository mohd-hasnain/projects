package com.aexp.cs.bulletproofing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
/***
 * 
 * @author Ravi.Mandholia
 *
 */
public class XmlParserDriver {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		// add resources
		//conf.addResource(new Path(args[2]));
		String queue_name=args[2];
		String start_end_tag =args[3];
		String primaryKeys=args[4];
		
		//String queue_name = conf.get("queue_name");

		// queue_name from config file
		conf.set("mapred.job.queue.name", queue_name);
		conf.set("output_path", args[1]);
		conf.set("start_end_tag",start_end_tag);
		conf.set("primaryKeys",primaryKeys);
		//String start_end_tag = conf.get("start_end_tag");
		

		// for xml reading
		conf.set("xmlinput.start", "<" + start_end_tag);
		conf.set("xmlinput.end", "</" + start_end_tag + ">");
		conf.set(
				"io.serializations",
				"org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
		Job job = new Job(conf, "XML Unique Records");
		job.setJarByClass(XmlParserDriver.class);
		job.setMapperClass(XmlParserMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setReducerClass(XmlParserReducer.class);
		MultipleOutputs.addNamedOutput(job, "duplicate", TextOutputFormat.class, Text.class, Text.class);

		//Setting input and output path
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		//custom input format is created to read xml file
		job.setInputFormatClass(XmlInputFormat.class);

		//default outputformat
		job.setOutputFormatClass(TextOutputFormat.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
