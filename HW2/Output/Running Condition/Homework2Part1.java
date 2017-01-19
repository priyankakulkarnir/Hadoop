package ucsc.hadoop.homework2;

/* Imports Added */

import ucsc.hadoop.util.ConfigurationUtil;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Homework2Part1 extends Configured implements Tool {

	private static final Log LOGGER1 = LogFactory.getLog(Homework2Part1.class);

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		System.out.println(Homework2Part1.class.getName());
		int exitpart1 = ToolRunner.run(new Homework2Part1(), args);
		System.exit(exitpart1);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration config = getConf();
		if (arg0.length != 2) {
			System.err.println("Usage: Homework2Part1 <in> <out>");
			System.exit(2);
		}
		ConfigurationUtil.dumpConfigurations(config, System.out);

		LOGGER1.info("input: " + arg0[0] + " output: " + arg0[1]);

		//@SuppressWarnings("deprecation")
		//Job job = new Job(config, "Homework2Part1");
		Job job = Job.getInstance();
		
		job.setJarByClass(Homework2Part1.class);
		job.setMapperClass(MovieTokenizerMapper.class);
		job.setReducerClass(MovieActorReducer.class);

		job.setJobName("Homework2Part1");
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));

		boolean result = job.waitForCompletion(true);
		return (result) ? 0 : 1;
	}

	public static class MovieTokenizerMapper extends
			Mapper<Object, Text, Text, Text> {
		private final static Text MOVIE = new Text();
		private final static Text ACTOR = new Text();

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\\t");

			if (tokens.length == 3) {
				String movie = tokens[1];
				String actor = tokens[0];
				MOVIE.set(movie);
				ACTOR.set(actor);
				context.write(MOVIE, ACTOR);
			}
		}
	}

	public static class MovieActorReducer extends
			Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		@Override
		public void reduce(Text movie, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			String actors = "";
			for (Text actor : values) {
				actors = actors + actor + "; ";
			}
			result.set(actors);
			context.write(movie, result);
		}
	}
}
