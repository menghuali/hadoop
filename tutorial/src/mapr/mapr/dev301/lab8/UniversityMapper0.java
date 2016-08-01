package mapr.dev301.lab8;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UniversityMapper0 extends Mapper<LongWritable, Text, NullWritable, Text> {

	private static String VERBAL_FIELD_BEGIN = "sat verbal";
	private static String MATH_FIELD_BEGIN = "sat math";
	private static char FIELD_END = ')';
	private static final Log LOG = LogFactory.getLog(UniversityMapper0.class);

	private Text outv = new Text();

	static enum CounterEnum {
		MISS_VERBAL, MISS_MATH, BAD_VERBAL, BAD_MATH
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String record = value.toString();

		// test if record has verbal
		int verbalBeginIndex = record.indexOf(VERBAL_FIELD_BEGIN);
		if (verbalBeginIndex < 0) {
			context.getCounter(CounterEnum.MISS_VERBAL).increment(1);
			LOG.debug("miss verbal: " + key.get());
		}

		// test if record has math
		int mathBeginIndex = record.indexOf(MATH_FIELD_BEGIN);
		if (verbalBeginIndex >= 0 && mathBeginIndex < 0) {
			context.getCounter(CounterEnum.MISS_MATH).increment(1);
			LOG.debug("miss math: " + key.get());
		}

		// return if no verbal or math
		if (verbalBeginIndex < 0 || mathBeginIndex < 0) {
			return;
		}

		boolean badValue = false; // bad value flag

		// get verbal value
		int verbal = 0;
		try {
			verbal = Integer.parseInt(record.substring(verbalBeginIndex + VERBAL_FIELD_BEGIN.length(),
					record.indexOf(FIELD_END, verbalBeginIndex)).trim());
		} catch (Throwable e) {
			context.getCounter(CounterEnum.BAD_VERBAL).increment(1);
			LOG.debug("bad verbal: " + key.get());
			badValue = true;
		}

		// get math value
		int math = 0;
		try {
			math = Integer.parseInt(record
					.substring(mathBeginIndex + MATH_FIELD_BEGIN.length(), record.indexOf(FIELD_END, mathBeginIndex))
					.trim());
		} catch (Throwable e) {
			context.getCounter(CounterEnum.BAD_MATH).increment(1);
			LOG.debug("bad math: " + key.get());
			badValue = true;
		}

		// return if bad verbal or math value
		if (badValue) {
			return;
		}

		// output verbal value
		outv.set(verbal + "," + math);
		context.write(NullWritable.get(), outv);
	}

}
