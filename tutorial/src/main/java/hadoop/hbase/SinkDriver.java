package hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SinkDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 1) {
			System.err.printf("Usage: %s [generic options] <input>\n", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		Configuration conf = HBaseConfiguration.create(getConf());
		Job job = Job.getInstance(conf, "hbase.sink");
		job.setJarByClass(SinkDriver.class);

		job.setMapperClass(SinkMapper.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));

		TableMapReduceUtil.initTableReducerJob("test", null, job);
		job.setNumReduceTasks(0);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new SinkDriver(), args));
	}

}
