package hadoopTut.hadoopTut;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RecordCountMR extends Configured implements Tool  {
	 	 public int run(String[] arg0) throws Exception {
			Configuration conf=getConf();
			conf.addResource("C:/BigDataSetups/hadoop-2.7.2/etc/hadoop/mapred-site.xml");
			Job job=Job.getInstance(conf);
			job.setJarByClass(RecordCountMR.class);
			job.setMapperClass(RecordCountMRMapper.class);
			job.setReducerClass(RecordCountMRReducer.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(LongWritable.class);
			Path output = new Path("/Users/Amit/hadoop-files/MROutput/");
			FileSystem hdfs = FileSystem.get(conf);
			if (hdfs.exists(output)) {
			    hdfs.delete(output, true);
			}
			FileInputFormat.setInputPaths(job, new Path("/Users/Amit/hadoop-files/recordCount/input/deckofcards.txt"));
			FileOutputFormat.setOutputPath(job, new Path("/Users/Amit/hadoop-files/recordCount/MROutput/"));
			return job.waitForCompletion(true) ? 0 : 1;
		}
		public static void main(String[] args) throws Exception{
			System.exit(ToolRunner.run(new RecordCountMR(), args));
		}
}

