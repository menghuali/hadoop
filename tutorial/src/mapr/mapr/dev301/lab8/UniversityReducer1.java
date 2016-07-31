package mapr.dev301.lab8;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UniversityReducer1 extends Reducer<Text, IntWritable, Text, FloatWritable> {

	static final String MEAN_POSTFIX = "_mean";
	private Text outk = new Text();
	private FloatWritable outv = new FloatWritable();

	@Override
	protected void reduce(Text key, Iterable<IntWritable> value, Context context)
			throws IOException, InterruptedException {
		long sum = 0;
		long count = 0;
		for (IntWritable v : value) {
			sum += v.get();
			count++;
		}
		outk.set(key.toString() + MEAN_POSTFIX);
		outv.set(sum / count);
		context.write(outk, outv);
	}

}
