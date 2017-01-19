package ucsc.hadoop.trial1;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import ucsc.hadoop.util.ConfigurationUtil;


/**
 * Simple MapReduce application to count how many movies per year
 * 
 *
 */
public class part1 extends Configured implements Tool{
	
	private static final Log LOG = LogFactory.getLog(part1.class);
	
	public int run(String[] args) throws Exception {
		
		if (args.length != 2) {
			System.err.println("Usage: moviecount <in> <out>");
			System.exit(2);
		}
		
		Configuration conf = getConf();
		ConfigurationUtil.dumpConfigurations(conf, System.out);
		
		LOG.info("input: " + args[0] + " output: " + args[1]);
		
		Job job = new Job(conf, "movie count");
		job.setJarByClass(part1.class);
		job.setMapperClass(TemplateMapper.class);
		job.setReducerClass(TemplateReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean result = job.waitForCompletion(true);
		return (result) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new part1(), args);
		System.exit(exitCode);
	}
	
	public static class TemplateMapper extends Mapper <Object, Text, Text, Text>{
		
		@Override
		public void setup(Context context) {
			System.out.println("********** in setup method *************");
		}
	
		@Override
		public void map(Object actor, Text movies, Context context) 
				throws IOException, InterruptedException {
			String[] seperateMovies = movies.toString().split("\\t");
			context.write(new Text(seperateMovies[1]), new Text(seperateMovies[0]));
		}
	}
	
	public static class TemplateReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text movie, Iterable <Text> actors, Context context) throws IOException, InterruptedException {
			 StringBuilder actorsList = new StringBuilder();
          
            for (Text actor : actors) {
                if (actorsList.length() > 0){
                    actorsList.append(";");
                }
                actorsList.append(actor.toString());
            }
            context.write(movie, new Text(actorsList.toString()));
        
		}
}
}