package mapr.dev301.lab4;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import java.io.IOException;

public class UniversityReducer extends Reducer<Text, IntWritable, Text, FloatWritable> {

	private Text resultKey = new Text();
	private FloatWritable resultValue = new FloatWritable();

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		int count = 0;
		int min = Integer.MAX_VALUE;
		int max = Integer.MIN_VALUE;
		for (IntWritable iw : values) {
			int value = iw.get();
			min = Math.min(min, value);
			max = Math.max(max, value);
			sum += value;
			count++;
		}

		String keyStr = key.toString();

		resultKey.set(keyStr + "_min");
		resultValue.set(min);
		context.write(resultKey, resultValue);

		resultKey.set(keyStr + "_max");
		resultValue.set(max);
		context.write(resultKey, resultValue);

		resultKey.set(keyStr + "_mean");
		resultValue.set(sum / count);
		context.write(resultKey, resultValue);
	}
}
