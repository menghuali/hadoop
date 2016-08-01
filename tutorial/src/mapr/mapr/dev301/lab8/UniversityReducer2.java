package mapr.dev301.lab8;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UniversityReducer2 extends Reducer<Text, FloatWritable, Text, FloatWritable> {
	private Text outk = new Text();
	private FloatWritable outv = new FloatWritable();

	@Override
	protected void reduce(Text key, Iterable<FloatWritable> value, Context context)
			throws IOException, InterruptedException {
		float sum = 0;
		for (FloatWritable v : value) {
			sum += v.get();
		}
		outk.set("SUM_" + key.toString());
		outv.set(sum);
		context.write(outk, outv);
	}

}
