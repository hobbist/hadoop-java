package hadoopTut.hadoopTut;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RecordCountMRReducer extends Reducer<Text, LongWritable, Text, LongWritable>{

	@Override
	protected void reduce(Text arg0, Iterable<LongWritable> arg1,Context arg2) throws IOException, InterruptedException {
		Iterator<LongWritable> itr=arg1.iterator();
		long sum=0;
		while(itr.hasNext()){
			sum+=itr.next().get();
		}
		arg2.write(arg0, new LongWritable(sum));
	}
 }
