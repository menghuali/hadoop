package mapr.dev301.lab8;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UniversityMapper2 extends Mapper<LongWritable, Text, Text, FloatWritable> {

	static final Text OUT_KEY_XY = new Text("XY");
	static final Text OUT_KEY_X2 = new Text("X2");
	static final Text OUT_KEY_Y2 = new Text("Y2");
	static final String CONF_VERBAL_MEAN = "UniversityMapper2.VerbalMean";
	static final String CONF_MATH_MEAN = "UniversityMapper2.MathMean";

	private FloatWritable outv = new FloatWritable();
	private float verbalMean;
	private float mathMean;

	static enum CounterEnum {
		BAD_RECORD, BAD_VERBAL, BAD_MATH
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		Configuration conf = context.getConfiguration();
		verbalMean = Float.parseFloat(conf.get(CONF_VERBAL_MEAN));
		mathMean = Float.parseFloat(conf.get(CONF_MATH_MEAN));
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
		float verbal = 0;
		try {
			verbal = Float.parseFloat(split[0]);
		} catch (NumberFormatException e1) {
			context.getCounter(CounterEnum.BAD_VERBAL).increment(1);
			badValue = true;
		}

		// get math value
		float math = 0;
		try {
			math = Float.parseFloat(split[1]);
		} catch (Throwable e) {
			context.getCounter(CounterEnum.BAD_MATH).increment(1);
			badValue = true;
		}

		// return if bad verbal or math value
		if (badValue) {
			return;
		}

		float x = verbal - verbalMean;
		float y = math - mathMean;

		outv.set(x * y);
		context.write(OUT_KEY_XY, outv);

		outv.set(x * x);
		context.write(OUT_KEY_X2, outv);

		outv.set(y * y);
		context.write(OUT_KEY_Y2, outv);
	}

}
