package hadoop.hbase;

import static hadoop.hbase.TableConstant.Stats.*;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SinkReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {

	@Override
	protected void reduce(Text bizname, Iterable<IntWritable> num,
			Reducer<Text, IntWritable, ImmutableBytesWritable, Mutation>.Context ctx)
			throws IOException, InterruptedException {
		int total = 0;
		for (IntWritable n : num) {
			total += n.get();
		}
		Put put = new Put(Bytes.toBytes(bizname.toString()));
		put.addColumn(FAMILY, COL_NUM, Bytes.toBytes(total));
		ctx.write(null, put);
	}

}
