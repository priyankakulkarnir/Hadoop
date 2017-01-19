package ucsc.hadoop.trial1;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import ucsc.hadoop.util.ConfigurationUtil;

public class part2 extends Configured implements Tool {


private static final Log LOG = LogFactory.getLog(part2.class);

public int run(String[] args) throws Exception {
Configuration conf = getConf();
System.out.println(args.length);
System.out.println(args[0]);
System.out.println(args[1]);
if (args.length != 2) {
System.exit(2);
}



LOG.info("input: " + args[0] + " output: " + args[1]);

Job job = new Job(conf, "job1");
job.setJarByClass(part2.class);

//map-reduce job 1
job.setMapperClass(MovieTokenizerMapper.class);
job.setNumReduceTasks(1);	
job.setReducerClass(ActorMovieCountReducer.class);
job.setMapOutputKeyClass(Text.class);
job.setMapOutputValueClass(IntWritable.class);

job.setOutputKeyClass(Text.class);
job.setOutputValueClass(IntWritable.class);

FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
String job1Output = new String(args[1]);
String job2Output = job1Output + "/outputfrommapper1";

boolean resultFromJob1 = job.waitForCompletion(true);

//Map reduce job 1 
Job job2 = new Job(conf, "job2");
job2.setJarByClass(part2.class);
job2.setMapperClass(ActorMovieCountMapper.class);
job2.setNumReduceTasks(1);	
job2.setReducerClass(SortReducer.class);
job2.setMapOutputKeyClass(IntWritable.class);
job2.setMapOutputValueClass(Text.class);

job2.setOutputKeyClass(IntWritable.class);
job2.setOutputValueClass(Text.class);
job2.setSortComparatorClass(DescSorter.class);

FileInputFormat.addInputPath(job2, new Path(job1Output + "/part-r-00000"));
FileOutputFormat.setOutputPath(job2, new Path(job2Output));

boolean resultFromJob2 = job2.waitForCompletion(true);

return (resultFromJob1 && resultFromJob2) ? 0 : 1;
}

public static void main(String[] args) throws Exception {
int exitCode = ToolRunner.run(new part2(), args);
System.exit(exitCode);
}

//map1 and reducer1 actually does the first step of the assignment, which is counting the no of movies each actor have acted in
public static class MovieTokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
private static IntWritable ONE = new IntWritable(1);
private static Text actor = new Text();


@Override
public void map(Object key, Text value, Context context)
throws IOException, InterruptedException {
String[] tokens = value.toString().split("\\t");

if (tokens.length == 3) {
actor.set(tokens[0]);
context.write(actor, ONE);
}
}
}

//reducer1 
public static class ActorMovieCountReducer extends Reducer< Text,IntWritable, IntWritable,Text> {
private static IntWritable count = new IntWritable();

@Override
public void reduce(Text actor,Iterable<IntWritable> values, Context context)
throws IOException, InterruptedException {

int countForActor = 0;
for (IntWritable count : values) {
countForActor += count.get();
}
count.set(countForActor);
context.write(count,actor);


}
}
//mapper2 and reducer2 does the sorting. 
public static class ActorMovieCountMapper extends Mapper<Object, Text, IntWritable, Text> {
private static IntWritable count = new IntWritable();
private static Text actor = new Text();


@Override
public void map(Object key, Text value, Context context)
throws IOException, InterruptedException {
String[] tokens = value.toString().split("\\t");
if (tokens.length == 2) {
count.set(Integer.parseInt(tokens[0]));
actor.set(tokens[1]);
context.write(count, actor);
} else {
System.exit(1);

}
}
}
//reducer2
public static class SortReducer
extends Reducer<IntWritable, Text, IntWritable, Text> {


public void reduce(IntWritable key, Text value, OutputCollector<IntWritable, Text> out,Context context) throws IOException, InterruptedException {
context.write(key,value);
}
}

public static class DescSorter extends IntWritable.Comparator {
public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
return -super.compare(b1, s1, l1, b2, s2, l2);
}
static {
WritableComparator.define(DescSorter.class,
new IntWritable.Comparator());
}

}
}
