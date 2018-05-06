package dbJobs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.util.Tool;

/*
 * Job class to get values from database(contactsInfo) and write to database(contacts)
 */

public class ContactInfoJob extends Configured implements Tool {
	public static void main(String[] args) {
		try {
			new ContactInfoJob().run(args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		job.setJarByClass(getClass());
		job.setInputFormatClass(DBInputFormat.class);
		job.setOutputFormatClass(DBOutputFormat.class);
		//mapper setup
		job.setMapperClass(ContactMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ContactInfo.class);
		//reducer setup
		job.setReducerClass(ContactReducer.class);
		
		DBConfiguration.configureDB(job.getConfiguration(), "com.mysql.jdbc.Driver", "jdbc:mysql://localhost/mydb", "root","admin");
		DBInputFormat.setInput(job, ContactInfo.class, "contactsInfo", null, null, "id","active");
		DBOutputFormat.setOutput(job, "contacts", "id","active");
		return job.waitForCompletion(true)? 1:0;
	}
	
	private static class ContactMapper extends Mapper<LongWritable, ContactInfo, Text, ContactInfo>{

		@Override
		protected void map(LongWritable key, ContactInfo value,Context context)
				throws IOException, InterruptedException {
				context.write(new Text(value.getId()), value);
		}
		
	}
	
	private static class ContactReducer extends Reducer<Text, ContactInfo, ContactInfo, NullWritable>{

		@Override
		protected void reduce(Text key, Iterable<ContactInfo> values,Context context) throws IOException, InterruptedException {
			for(ContactInfo con:values){
				context.write(con, NullWritable.get());
			}
		}
		
	}

}
