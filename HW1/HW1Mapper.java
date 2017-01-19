package edu.ucsc.hadoop30088.hw1;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Add import statements as needed
 */

/**
 * Notice that the inputs and outputs are now just "Text". You may need to
 * replace these with a different Writable (hint: values are integers)
 */

/**
 * To define a map function for your MapReduce job, subclass the Mapper class
 * and override the map method. The class definition requires four parameters:
 * @param The data type of the input key - LongWritable
 * @param The data type of the input value - Text
 * @param The data type of the output key - Text
 * @param The data type of the output value - LongWritable
 */

public class HW1Mapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	/*
	 * Declaring one as LogWritable as static final
	 */
	private final static LongWritable one = new LongWritable(1);

	/**
	 * The map method runs once for each line of text in the input file. The
	 * method receives:
	 * @param A key of type LongWritable
	 * @param A value of type Text
	 * @param A Context object.
	 */
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		/*
		 * Convert the line, which is received as a Text object, to a String
		 * object.
		 */

		String line = value.toString();

		/*
		 * The line.split("\\W+") call uses regular expressions to split the
		 * line up by non-word characters.
		 */
		for (String word : line.split("\\W+")) {
			if (word.length() > 0) {

				/*
				 * Obtain the first letter of the word and convert into lower
				 * case
				 */
				String letter = word.substring(0, 1).toLowerCase();

				/*
				 * Call the write method on the Context object to emit a key and
				 * a value from the map method. The key is the letter (in
				 * lower-case) that the word starts with; the value is the
				 * number of words starting with that alphabet.
				 */
				context.write(new Text(letter), one);
			}
		}

	}
}
