package hadoopTut.hadoopTut;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RecordCountMRMapper extends Mapper<Object, Text, Text, LongWritable>{
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
			context.write(new Text("Count"), new LongWritable(1));
	}
 }