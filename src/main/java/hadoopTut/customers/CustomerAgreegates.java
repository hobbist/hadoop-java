package hadoopTut.customers;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
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

import hadoopTut.hadoopTut.RecordCountMR;


public class CustomerAgreegates extends Configured implements Tool {

	private static class CustomerMapper extends Mapper<Object, Text, IntWritable, FloatWritable> {

		@Override
		protected void map(Object key, Text value,Context context)
				throws IOException, InterruptedException { 
			String[] values=value.toString().split(",");
			IntWritable custId=new IntWritable(Integer.valueOf(values[0]));
			FloatWritable purchaseValue=new FloatWritable(Float.valueOf(values[2]));
			context.write(custId, purchaseValue);
		}
	}
	
	private static class CustomerReducer extends Reducer<IntWritable, FloatWritable, IntWritable, FloatWritable>{

		@Override
		protected void reduce(IntWritable custId, Iterable<FloatWritable> purchases,Context context)
				throws IOException, InterruptedException {
			Float totalPurchase=new Float(0);
			Iterator<FloatWritable> purchaseItr=purchases.iterator();
			while(purchaseItr.hasNext()){
				totalPurchase+=purchaseItr.next().get();
			}
			context.write(custId, new FloatWritable(totalPurchase));
		}
	}
	
	public static void main(String[] args) throws Exception{
		System.exit(ToolRunner.run(new CustomerAgreegates(), args));	
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf=getConf();
		conf.addResource("C:/BigDataSetups/hadoop-2.7.2/etc/hadoop/mapred-site.xml");
		Job job=Job.getInstance(conf);
		job.setJarByClass(CustomerAgreegates.class);
		job.setMapperClass(CustomerMapper.class);
		job.setReducerClass(CustomerReducer.class);
		//Map Output key and value classes
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(FloatWritable.class);
		//Final output classes 
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(FloatWritable.class);
		Path output = new Path("/customers/output/");
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(output)) {
		    hdfs.delete(output, true);
		}
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		//input and output paths
		FileInputFormat.setInputPaths(job, new Path("/Users/Amit/hadoop-files/customers/orders.csv"));
		FileOutputFormat.setOutputPath(job, new Path("/Users/Amit/hadoop-files/customers/output/"));
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
