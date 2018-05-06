package dbJobs;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


//Aggregate contacts based on activeness
public class ContactAgreegation extends Configured implements Tool {
	public static void main(String[] args) {
		try {
			System.exit(ToolRunner.run(new ContactAgreegation(), args));
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
		job.setOutputFormatClass(TextOutputFormat.class);
		//mapper setup
		job.setMapperClass(ContactAggMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ContactInfo.class);
		//reducer setup
		job.setReducerClass(ContactAggReducer.class);
		//sort based on values
		DBConfiguration.configureDB(job.getConfiguration(), "com.mysql.jdbc.Driver", "jdbc:mysql://localhost/mydb", "root","admin");
		DBInputFormat.setInput(job, ContactInfo.class, "contactsInfo", null, null, "id","active");
		Path output=new Path("/Users/Amit/hadoop-files/contactAgg/");
		FileOutputFormat.setOutputPath(job, new Path("/Users/Amit/hadoop-files/contactAgg/"));
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(output)) {
		    hdfs.delete(output, true);
		}
		
		return job.waitForCompletion(true)? 1:0;
	}
	
	private static class ContactAggMapper extends Mapper<LongWritable, ContactInfo, Text, ContactInfo>{

		@Override
		protected void map(LongWritable key, ContactInfo value,Context context)
				throws IOException, InterruptedException {
				context.write(new Text(value.getActive()), value);
		}
		
	}
	
	private static class ContactAggReducer extends Reducer<Text, ContactInfo, Text,Text>{

		@Override
		protected void reduce(Text key, Iterable<ContactInfo> values,Context context) throws IOException, InterruptedException {
			String str="";
			for(ContactInfo con:values){
				str=str+con.getId()+",";
				
			}
			str=str.substring(0, str.length()-1);
			context.write(key,new Text(str));
		}
		
	}

}