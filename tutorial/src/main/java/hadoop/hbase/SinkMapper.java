package hadoop.hbase;

import static hadoop.hbase.TableConstant.YellowPage.*;

import java.io.IOException;
import java.text.ParseException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SinkMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
	private static final Logger LOG = Logger.getAnonymousLogger();

	private static final String SEPARATE1 = "..";
	private static final String SEPARATE2 = "…………………";

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, ImmutableBytesWritable, Put>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] data;
		try {
			data = parseLine(line);
		} catch (ParseException e) {
			LOG.log(Level.WARNING, "Failed to parse: " + line, e);
			return;
		}
		Put put = new Put(Bytes.toBytes(data[0]));
		put.addColumn(FAMILY, COL_NAME, Bytes.toBytes(data[1]));
		put.addColumn(FAMILY, COL_ADDR, Bytes.toBytes(data[2]));
		context.write(new ImmutableBytesWritable(put.getRow()), put);
	}

	static String[] parseLine(String line) throws ParseException {
		String[] result = new String[3];
		int end = line.indexOf(SEPARATE1);
		if (end <= 0) {
			throw new ParseException("Cannot find phone", 0);
		}
		result[0] = line.substring(0, end);
		int start = end + SEPARATE1.length();
		end = line.indexOf(SEPARATE2, start);
		if (end < 0) {
			throw new ParseException("Cannot find business name", start);
		}
		result[1] = line.substring(start, end);
		start = end + SEPARATE2.length();
		if (start >= line.length()) {
			throw new ParseException("Cannot find address", start);
		}
		result[2] = line.substring(start);
		return result;
	}

}
