package multiple;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import dbJobs.ContactInfo;

public class MultipleInputsJob extends Configured implements Tool {
	public static void main(String[] args) {
		try {
			System.exit(ToolRunner.run(new MultipleInputsJob(), args));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		job.setJarByClass(getClass());
		job.setOutputFormatClass(TextOutputFormat.class);
		//mapper setup
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		//reducer setup
		job.setReducerClass(ContactMultipleFormatReducer.class);
		//Multiple input datasources
		//MultipleInputs.addInputPath(job, null, DBInputFormat.class,ContactDbMapper.class);
		MultipleInputs.addInputPath(job, new Path("/Users/Amit/hadoop-files/contactFile/Input.txt"), TextInputFormat.class,ContactFileMapper.class);
		
		//DBConfiguration.configureDB(job.getConfiguration(), "com.mysql.jdbc.Driver", "jdbc:mysql://localhost/mydb", "root","admin");
		//DBInputFormat.setInput(job, ContactInfo.class, "contactsInfo", null, null, "id","active");
		Path output=new Path("/Users/Amit/hadoop-files/contact-multiple/");
		FileOutputFormat.setOutputPath(job, new Path("/Users/Amit/hadoop-files/contact-multiple/"));
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(output)) {
		    hdfs.delete(output, true);
		}
		
		return job.waitForCompletion(true)? 1:0;
	}
	
	private static class ContactDbMapper extends Mapper<LongWritable, ContactInfo, Text, Text>{

		@Override
		protected void map(LongWritable key, ContactInfo value,Context context) throws IOException, InterruptedException {
			context.write(new Text(value.getActive()), new Text(value.getId()));
		}
		
	}
	
	private static class ContactFileMapper extends Mapper<LongWritable, Text, Text, Text>{

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] values=value.toString().split(",");
			context.write(new Text(values[1]), new Text(values[0]));
		}
		
	}
	
	private static class ContactMultipleFormatReducer extends Reducer<Text,Text,Text,Text>{

		@Override
		protected void reduce(Text arg0, Iterable<Text> arg1, Context arg2) throws IOException, InterruptedException {
			for(Text val:arg1){
				arg2.write(arg0, val);
			}
		}
		
	}

}
