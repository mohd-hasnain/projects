package com.aexp.cs.bulletproofing;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/***
 * 
 * @author Ravi.Mandholia
 *
 */
public class DelimitedParserMapper extends Mapper<LongWritable, Text, Text, Text> {

	String primaryKeys[];
	String delimiter;

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {

		// reading primary keys from config file
		primaryKeys = context.getConfiguration().get("primaryKeys").split(",");

		// reading delimiter from config file
		delimiter = context.getConfiguration().get("delimiter");

	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] delimitedString = value.toString().split(delimiter);
		String out1 = "";
		

			for (int i = 0; i < primaryKeys.length; i++) {
				if (i == 0) {
					if (delimitedString[Integer.parseInt(primaryKeys[i])] != null)
						out1 = delimitedString[Integer.parseInt(primaryKeys[i])];
				} else {
					if (delimitedString[Integer.parseInt(primaryKeys[i])] != null)
						out1 = out1 + "_" + delimitedString[Integer.parseInt(primaryKeys[i])];
				}
			}

			context.write(new Text(out1), value);

	}

}
