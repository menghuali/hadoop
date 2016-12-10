package hadoop.hbase;

import java.io.IOException;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SinkMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

	private static final byte[] FAMILY = Bytes.toBytes("cf");
	private static final byte[] QUALIFIER = Bytes.toBytes("col1");

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, ImmutableBytesWritable, Put>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		Put put = new Put(DigestUtils.md5(line));
		put.addColumn(FAMILY, QUALIFIER, Bytes.toBytes(line));
		context.write(new ImmutableBytesWritable(put.getRow()), put);
	}

}
