package mapr.dev301.lab5;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

public class VoterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private static Log log = LogFactory.getLog(VoterMapper.class);
	static final String COUNTER_GROUP = "mapr.dev301";
	static final String COUTNER_NAME_MISSING_FIELDS = "lab05.voter.missing_fields";
	static final String COUTNER_NAME_BAD_AGE = "lab05.voter.bad_age";

	private Text outk = new Text();
	private IntWritable outv = new IntWritable();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// create iterator over record assuming space-separated fields
		StringTokenizer iterator = new StringTokenizer(value.toString(), ",");

		int fieldsAmt = iterator.countTokens();
		String recordNum = iterator.nextToken();

		if (fieldsAmt < 6) {
			Counter counter = context.getCounter(COUNTER_GROUP, COUTNER_NAME_MISSING_FIELDS);
			counter.increment(1);
			log.warn("Invalid record: " + recordNum);
			return;
		}

		iterator.nextToken(); // ignore name

		String ageText = iterator.nextToken().trim();
		int age = 0;
		boolean invalidAgeText = false;
		try {
			age = Integer.parseInt(ageText);
		} catch (NumberFormatException e) {
			invalidAgeText = true;
		}
		if (invalidAgeText || age < 18 || age > 200) {
			Counter counter = context.getCounter(COUNTER_GROUP, COUTNER_NAME_BAD_AGE);
			counter.increment(1);
			log.warn("Invalid age: " + recordNum + "," + ageText);
			return;
		}
		outv.set(age);
		outk.set(iterator.nextToken());
		context.write(outk, outv);
	}

}
