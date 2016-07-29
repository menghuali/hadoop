package mapr.dev301.lab5;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import java.io.IOException;

public class VoterReducer extends Reducer<Text, IntWritable, Text, FloatWritable> {
	private Text outk = new Text();
	private FloatWritable outv = new FloatWritable();

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		long sum = 0L;
		int count = 0;
		int max = Integer.MIN_VALUE;
		int min = Integer.MAX_VALUE;
		for (IntWritable value : values) {
			int age = value.get();
			max = Math.max(max, age);
			min = Math.min(min, age);
			sum += age;
			count++;

		}
		String prefix = key.toString();

		outk.set(prefix + "_age_mean");
		outv.set(sum / count);
		context.write(outk, outv);

		outk.set(prefix + "_age_min");
		outv.set(min);
		context.write(outk, outv);

		outk.set(prefix + "_age_max");
		outv.set(max);
		context.write(outk, outv);
	}
}
