package ucsc.hadoop.homework2;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import ucsc.hadoop.util.ConfigurationUtil;

public class Homework2Part2 extends Configured implements Tool{

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		System.out.println(Homework2Part2.class.getName());
		
		int exitCode = ToolRunner.run(new Homework2Part2(), args);
		System.exit(exitCode);
	}
	
private static final Log LOG = LogFactory.getLog(Homework2Part2.class);
	
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		if (args.length != 2) {
			System.err.println("Usage: Homework2Part2 <in> <out>");
			System.exit(2);
		}
		
		ConfigurationUtil.dumpConfigurations(conf, System.out);
		
		LOG.info("input: " + args[0] + " output: " + args[1]);
		
		Job job = new Job(conf, "Homework2Part2");
		job.setJarByClass(Homework2Part2.class);
		job.setMapperClass(MovieTokenizerMapper.class);
		job.setReducerClass(MovieCountReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "tmp"));
		
		//job2
		
		Job job2 = new Job(conf, "Homework2Part2 job2");
		
		job2.setJarByClass(Homework2Part2.class);
		job2.setSortComparatorClass(SortIntComparator.class);

		job2.setMapperClass(CountActorMapper.class);
		
		//job2.setReducerClass(MovieYearReducer2.class);
		//job2.setNumReduceTasks(0);

		job2.setMapOutputKeyClass(IntWritable.class);
		job2.setMapOutputValueClass(Text.class);
		
		//job2.setOutputKeyClass(IntWritable.class);
		//job2.setOutputValueClass(Text.class);
		
		
		FileInputFormat.addInputPath(job2, new Path(args[1] + "tmp"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		
		boolean result = job.waitForCompletion(true) && job2.waitForCompletion(true);
		return (result) ? 0 : 1;
	}
	
	
	public static class MovieTokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
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
	
	public static class MovieCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		
		@Override
		public void reduce(Text actor, Iterable<IntWritable> values, Context context) 
				 throws IOException, InterruptedException {
				
			int movieCountPerActor = 0;
			for (IntWritable count : values) {
				movieCountPerActor += count.get();
			}
			result.set(movieCountPerActor);
			context.write(actor, result);
		}
	}
	

	
	public static class CountActorMapper extends Mapper<Object, Text, IntWritable, Text> {
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
	
	public static class SortIntComparator extends WritableComparator {
		//Constructor.
		protected SortIntComparator() {
			super(IntWritable.class, true);
		}
	 
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			IntWritable k1 = (IntWritable)w1;
			IntWritable k2 = (IntWritable)w2;
			
			return -1 * k1.compareTo(k2);
		}
	}
	
//	public static class MovieYearReducer2 extends Reducer<IntWritable, Text, IntWritable, Text> {
//	
//		
//		@Override
//		public void reduce(IntWritable count, Iterable<Text> values, Context context) 
//				 throws IOException, InterruptedException {
//				
//			for (Text actor : values) {
//				context.write(count, actor);
//			}
//		}
//		
//	}

}