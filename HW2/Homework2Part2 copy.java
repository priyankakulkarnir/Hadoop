package ucsc.hadoop.homework2;

import ucsc.hadoop.util.ConfigurationUtil;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Homework2Part2 extends Configured implements Tool {

	private static final Log LOGGER2 = LogFactory.getLog(Homework2Part2.class);

	public static void main(String[] args) throws Exception {
		System.out.println(Homework2Part2.class.getName());
		int exitpart2 = ToolRunner.run(new Homework2Part2(), args);
		System.exit(exitpart2);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		if (arg0.length != 2) {
			System.err.println("Usage: Homework2Part2 <in> <out>");
			System.exit(2);
		}

		ConfigurationUtil.dumpConfigurations(conf, System.out);
		LOGGER2.info("input: " + arg0[0] + " output: " + arg0[1]);

		Job job = Job.getInstance(getConf());
		job.setJarByClass(Homework2Part2.class);
		job.setJobName("Homework2Part2");

		job.setNumReduceTasks(27);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(MovieTokenizerMapper.class);
		job.setPartitionerClass(MoviePartitioner.class);
		job.setCombinerClass(MovieCountReducer.class);
		job.setReducerClass(MovieCountReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		Path inputFilePath = new Path(arg0[0]);
		Path outputFilePath = new Path(arg0[1]);

		FileInputFormat.setInputDirRecursive(job, true);

		FileInputFormat.addInputPath(job, inputFilePath);
		FileOutputFormat.setOutputPath(job, outputFilePath);
		FileSystem fs = FileSystem.newInstance(getConf());
		if (fs.exists(outputFilePath)) {
			fs.delete(outputFilePath, true);
		}
		return job.waitForCompletion(true) ? 0 : 1;

		/**
		 * CODE FOR SORTING OUTPUT AS PER MOVIE COUNT Job job2 =
		 * Job.getInstance(); job2.setJarByClass(Homework2Part2.class);
		 * job2.setSortComparatorClass(SortIntComparator.class);
		 * job2.setMapperClass(CountActorMapper.class);
		 * job2.setJobName("Homework2Part2 job2");
		 * job2.setMapOutputKeyClass(IntWritable.class);
		 * job2.setMapOutputValueClass(Text.class);
		 * FileInputFormat.addInputPath(job2, new Path(arg0[1] + "tmp"));
		 * FileOutputFormat.setOutputPath(job2, new Path(arg0[1])); boolean
		 * result = job.waitForCompletion(true) && job2.waitForCompletion(true);
		 * return (result) ? 0 : 1;
		 **/
	}

	// Mapper Class

	public static class MovieTokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {
		private final static Text ACTOR = new Text();
		private final static IntWritable ONE = new IntWritable(1);

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\\t");
			if (tokens.length == 3) {
				String actor = tokens[0];
				ACTOR.set(actor);
				context.write(ACTOR, ONE);
			}
		}
	}

	// Reducer Class

	public static class MovieCountReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		@Override
		public void reduce(Text actor, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int movieCountPerActor = 0;
			for (IntWritable count : values) {
				movieCountPerActor += count.get();
			}
			result.set(movieCountPerActor);
			context.write(actor, result);
		}
	}

	// Count Actor Mapper Class

	public static class CountActorMapper extends
			Mapper<Object, Text, IntWritable, Text> {
		private final static Text ACTOR = new Text();
		private final static IntWritable COUNT = new IntWritable();

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\\t");

			if (tokens.length == 2) {
				String actor = tokens[0];
				int count = Integer.parseInt(tokens[1]);
				ACTOR.set(actor);
				COUNT.set(count);
				context.write(COUNT, ACTOR);
			}
		}
	}

	// SortComparator For Sorting

	public static class SortIntComparator extends WritableComparator {
		protected SortIntComparator() {
			super(IntWritable.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			IntWritable k1 = (IntWritable) w1;
			IntWritable k2 = (IntWritable) w2;
			return -1 * k1.compareTo(k2);
		}
	}

	// Alphabetic Partitioner

	public static class MoviePartitioner extends Partitioner<Text, IntWritable> {

		@Override
		public int getPartition(Text key, IntWritable value, int numPartitions) {
			String word = key.toString();
			char letter = word.toLowerCase().charAt(0);
			int partitionNumber = 0;

			switch (letter) {
			case 'a':
				partitionNumber = 1;
				break;
			case 'b':
				partitionNumber = 2;
				break;
			case 'c':
				partitionNumber = 3;
				break;
			case 'd':
				partitionNumber = 4;
				break;
			case 'e':
				partitionNumber = 5;
				break;
			case 'f':
				partitionNumber = 6;
				break;
			case 'g':
				partitionNumber = 7;
				break;
			case 'h':
				partitionNumber = 8;
				break;
			case 'i':
				partitionNumber = 9;
				break;
			case 'j':
				partitionNumber = 10;
				break;
			case 'k':
				partitionNumber = 11;
				break;
			case 'l':
				partitionNumber = 12;
				break;
			case 'm':
				partitionNumber = 13;
				break;
			case 'n':
				partitionNumber = 14;
				break;
			case 'o':
				partitionNumber = 15;
				break;
			case 'p':
				partitionNumber = 16;
				break;
			case 'q':
				partitionNumber = 17;
				break;
			case 'r':
				partitionNumber = 18;
				break;
			case 's':
				partitionNumber = 19;
				break;
			case 't':
				partitionNumber = 20;
				break;
			case 'u':
				partitionNumber = 21;
				break;
			case 'v':
				partitionNumber = 22;
				break;
			case 'w':
				partitionNumber = 23;
				break;
			case 'x':
				partitionNumber = 24;
				break;
			case 'y':
				partitionNumber = 25;
				break;
			case 'z':
				partitionNumber = 26;
				break;
			default:
				partitionNumber = 0;
				break;
			}

			return partitionNumber;
		}
	}

}
