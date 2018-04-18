package hadoopTut.hadoopTut;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RecordCount extends Configured implements Tool {

	public int run(String[] arg0) throws Exception {
		Configuration conf=getConf();
		conf.addResource("C:/BigDataSetups/hadoop-2.7.2/etc/hadoop/mapred-site.xml");
		Job job=Job.getInstance(conf);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job, new Path("/Users/Amit/hadoop-files/recordCount/input/deckofcards.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/Users/Amit/hadoop-files/recordCount/output"));
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception{
		System.out.println(System.getProperties()); 
		System.exit(ToolRunner.run(new RecordCount(), args));
	}
}
