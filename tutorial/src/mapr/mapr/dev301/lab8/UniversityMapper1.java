package mapr.dev301.lab8;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UniversityMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {

	static final Text VERBAL_KEY = new Text("verbal");
	static final Text MATH_KEY = new Text("math");

	private IntWritable outv = new IntWritable();

	static enum CounterEnum {
		BAD_RECORD, BAD_VERBAL, BAD_MATH
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] split = value.toString().split(",");

		// test if record has verbal
		if (split.length != 2) {
			context.getCounter(CounterEnum.BAD_RECORD).increment(1);
			return;
		}

		boolean badValue = false;
		
		// get verbal value
		int verbal = 0;
		try {
			verbal = Integer.parseInt(split[0]);
		} catch (NumberFormatException e1) {
			context.getCounter(CounterEnum.BAD_VERBAL).increment(1);
			badValue = true;
		}

		// get math value
		int math = 0;
		try {
			math = Integer.parseInt(split[1]);
		} catch (Throwable e) {
			context.getCounter(CounterEnum.BAD_MATH).increment(1);
			badValue = true;
		}

		// return if bad verbal or math value
		if (badValue) {
			return;
		}

		// output verbal value
		outv.set(verbal);
		context.write(VERBAL_KEY, outv);

		// output math value
		outv.set(math);
		context.write(MATH_KEY, outv);
	}

}
