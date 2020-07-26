package com.aexp.cs.bulletproofing;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
/***
 * 
 * @author Ravi.Mandholia
 *
 */
public class DelimitedParserReducer extends Reducer<Text, Text, Text, Text> {
	private MultipleOutputs<Text, Text> multipleOutputs;
	private String output_path;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException{
		this.multipleOutputs = new MultipleOutputs<Text, Text>(context);
		output_path = context.getConfiguration().get("output_path");
	}

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		int count = 0;
		for (Text val : values) {
			if (count == 0) {
				context.write(new Text(val), new Text(""));
				count = 1;
			} else {
				//multipleOutputs.write(new Text(val), new Text(""), "duplicate_output");
				multipleOutputs.write("duplicate", new Text(val), new Text(""), output_path+"/DuplicateRecords/duplicate");
			}
		}
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException{
		
			this.multipleOutputs.close();
	}
}
