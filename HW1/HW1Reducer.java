package edu.ucsc.hadoop30088.hw1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Notice that the r inputs and outputs are now just "Text". You may need to
 * replace these with a different Writable (hint: values are integers)
 */

/**
 * To define a reduce function for your MapReduce job, subclass the Reducer
 * class and override the reduce method. The class definition requires four
 * parameters:
 * @param The data type of the input key - Text
 * @param The data type of the input value - LongWritable
 * @param The data type of the output key - Text
 * @param The data type of the output value - LongWritable
 */

public class HW1Reducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	/**
	 * The reduce method runs once for each key received from the shuffle and
	 * sort phase of the MapReduce framework. The method receives: 
	 * @param A key of type Text
	 * @param A set of values of type LongWritable
	 * @param A Context object
	 */

	@Override
	public void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {

		long count = 0;

		/*
		 * For each value in the set of values passed to us by the mapper:
		 */
		for (@SuppressWarnings("unused") LongWritable value : values) {

			/*
			 * Increment the count will give number of words
			 */

			count++;
		}

		/*
		 * Call the write method on the Context object to emit a key (the words'
		 * starting letter) and a value (the count of words starting with this
		 * letter) from the reduce method.
		 */

		context.write(key, new LongWritable(count));
	}
}
