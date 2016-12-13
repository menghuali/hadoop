package hadoop.hbase;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SourceReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text bizname, Iterable<IntWritable> amount,
			Reducer<Text, IntWritable, Text, IntWritable>.Context ctx) throws IOException, InterruptedException {
		int total = 0;
		for (IntWritable i : amount) {
			total += i.get();
		}
		ctx.write(bizname, new IntWritable(total));
	}

}
