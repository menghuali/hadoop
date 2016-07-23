package mapr.dev301.lab4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class UniversityDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("usage: hadoop jar University.jar University.UniversityDriver <inputfile> <outputdir>");
			return 1;
		}

		Configuration conf = getConf();
		conf.set("textinputformat.record.delimiter", "))");
		Job job = Job.getInstance(conf, UniversityDriver.class.getName());
		job.setJarByClass(getClass());
		job.setMapperClass(UniversityMapper.class);
		job.setReducerClass(UniversityReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new UniversityDriver(), args));
	}
}
