package hadoop.hbase;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import hadoop.hbase.TableConstant.Stats;
import hadoop.hbase.TableConstant.YellowPage;

public class SourceSinkDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(HBaseConfiguration.create(getConf()));
		job.setJarByClass(SourceSinkDriver.class);

		Scan scan = new Scan();
		scan.addColumn(YellowPage.FAMILY, YellowPage.COL_NAME);
		TableMapReduceUtil.initTableMapperJob(YellowPage.NAME, new Scan(), SourceMapper.class, Text.class,
				IntWritable.class, job);

		TableMapReduceUtil.initTableReducerJob(Stats.NAME.getNameAsString(), SinkReducer.class, job);
		job.setNumReduceTasks(1);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new SourceSinkDriver(), args));
	}

}
