package hadoop.hbase;

import static hadoop.hbase.TableConstant.YellowPage.*;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SourceMapper extends TableMapper<Text, IntWritable> {
	private static final IntWritable ONE = new IntWritable(1);

	@Override
	protected void map(ImmutableBytesWritable key, Result value,
			Mapper<ImmutableBytesWritable, Result, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		if (value.containsColumn(FAMILY, COL_NAME)) {
			context.write(new Text(Bytes.toString(value.getValue(FAMILY, COL_NAME))), ONE);
		}
	}

}
