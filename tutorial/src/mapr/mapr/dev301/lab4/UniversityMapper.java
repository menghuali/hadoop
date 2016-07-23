package mapr.dev301.lab4;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UniversityMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	static final String SAT_VERBAL = "(sat verbal";
	static final String SAT_MATH = "(sat math";
	private Text outputKey = new Text();
	private IntWritable outputValue = new IntWritable();

	private static final Log LOG = LogFactory.getLog(UniversityMapper.class);

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String record = value.toString();
		if (!record.contains(SAT_VERBAL) || !record.contains(SAT_MATH)) {
			LOG.warn("Record #" + key.get() + " does not have satv or satm. Ignore this record.");
			return;
		}
		try {
			outputKey.set("satv");
			outputValue.set(getIntFieldValueFromRecord(record, SAT_VERBAL));
			context.write(outputKey, outputValue);

			outputKey.set("satm");
			outputValue.set(getIntFieldValueFromRecord(record, SAT_MATH));
			context.write(outputKey, outputValue);
		} catch (Exception e) {
			LOG.warn("Failed to pass record #" + key.get(), e);
		}
	}

	static int getIntFieldValueFromRecord(String record, String field) throws Exception {
		int fromIndex = record.indexOf(field);
		if (fromIndex < 0) {
			throw new IllegalStateException("This record does not contain the field: " + field);
		}
		fromIndex += field.length();
		return Integer.parseInt(record.substring(fromIndex, record.indexOf(')', fromIndex)).trim());
	}

}
