package hadoopTut.countCards;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CountCards extends Configured implements Tool {
	
	private static class CardsSuitMapper extends Mapper<Object, Text, Text, LongWritable>{
		@Override
		protected void map(Object key, Text value,Context context)
				throws IOException, InterruptedException {
			String[] fields=value.toString().split("\\|");
			context.write(new Text(fields[1]), new LongWritable(1));
		}
	}
	
	private static class CardsSuitReducer extends Reducer<Text,LongWritable,Text,LongWritable>{

		@Override
		protected void reduce(Text key, Iterable<LongWritable> mapValues,Context context) throws IOException, InterruptedException {
			Iterator<LongWritable> itr=mapValues.iterator();
			long cardCount=0;
			while(itr.hasNext()){
				cardCount+=itr.next().get();
			}
			context.write(key, new LongWritable(cardCount));	
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf=getConf();
		conf.addResource("C:/BigDataSetups/hadoop-2.7.2/etc/hadoop/mapred-site.xml");
		Job job=Job.getInstance(conf);
		job.setJarByClass(CountCards.class);
		job.setMapperClass(CardsSuitMapper.class);
		job.setReducerClass(CardsSuitReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setNumReduceTasks(2);
		Path output = new Path("/Users/Amit/hadoop-files/card-suits/");
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(output)) {
		    hdfs.delete(output, true);
		}
		FileInputFormat.setInputPaths(job, new Path("/Users/Amit/hadoop-files/recordCount/input/deckofcards.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/Users/Amit/hadoop-files/recordCount/card-suits/"));
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception{
		System.exit(ToolRunner.run(new CountCards(), args));
	}

}
