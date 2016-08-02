package mapr.dev301.lab8;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class UniversityDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		if (args == null || args.length != 5) {
			System.out.println("usage: mapr.dev301.lab8.UniversityDriver <in1> <out1> <out2> <out3> <out_pcc>");
		}

		int result = runJob0(args[0], args[1]);
		if (result != 0)
			return result;

		result = runJob1(args[1], args[2]);
		if (result != 0)
			return result;

		result = runJob2(args[1], args[2], args[3]);
		if (result != 0)
			return result;

		return computePCC(args[3], args[4]);
	}

	int runJob0(String input, String out0) throws Exception {
		Configuration conf = getConf();
		conf.set("textinputformat.record.delimiter", "))");
		Job job = Job.getInstance(conf);
		job.setJarByClass(getClass());
		job.setMapperClass(UniversityMapper0.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(out0));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	int runJob1(String out0, String out1) throws Exception {
		Configuration conf = getConf();
		conf.set("textinputformat.record.delimiter", System.getProperty("line.separator"));
		Job job = Job.getInstance(getConf());
		job.setJarByClass(getClass());
		job.setMapperClass(UniversityMapper1.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(UniversityReducer1.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(out0));
		FileOutputFormat.setOutputPath(job, new Path(out1));
		return job.waitForCompletion(true) ? 0 : 2;
	}

	int runJob2(String out0, String out1, String out2) throws Exception {
		Configuration conf = getConf();
		FileSystem dfs = FileSystem.get(conf);
		BufferedReader reader = new BufferedReader(new InputStreamReader(dfs.open(new Path(out1 + "/part-r-00000"))));
		String line = null;
		try {
			while ((line = reader.readLine()) != null) {
				String[] split = line.split("\t");
				if ("verbal_mean".equals(split[0])) {
					conf.setFloat(UniversityMapper2.CONF_VERBAL_MEAN, Float.parseFloat(split[1]));
				} else if ("math_mean".equals(split[0])) {
					conf.setFloat(UniversityMapper2.CONF_MATH_MEAN, Float.parseFloat(split[1]));
				}
			}
		} finally {
			reader.close();
		}

		Job job = Job.getInstance(getConf());
		job.setJarByClass(getClass());
		job.setMapperClass(UniversityMapper2.class);
		job.setReducerClass(UniversityReducer2.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(out0));
		FileOutputFormat.setOutputPath(job, new Path(out2));
		return job.waitForCompletion(true) ? 0 : 3;
	}

	int computePCC(String out2, String outPcc) throws Exception {
		float sumXY = Float.NaN, sumX2 = Float.NaN, sumY2 = Float.NaN;
		FileSystem dfs = FileSystem.get(getConf());
		BufferedReader reader = new BufferedReader(new InputStreamReader(dfs.open(new Path(out2 + "/part-r-00000"))));
		String line = null;
		while ((line = reader.readLine()) != null) {
			String[] split = line.split("\t");
			if ("SUM_XY".equals(split[0])) {
				sumXY = Float.parseFloat(split[1]);
			} else if ("SUM_X2".equals(split[0])) {
				sumX2 = Float.parseFloat(split[1]);
			} else if ("SUM_Y2".equals(split[0])) {
				sumY2 = Float.parseFloat(split[1]);
			}
		}

		BufferedWriter writer = null;
		try {
			writer = new BufferedWriter(new OutputStreamWriter(dfs.create(new Path(outPcc))));
			writer.write(Float.toString(sumXY / (float) Math.sqrt(sumX2 * sumY2)));
			writer.newLine();
		} finally {
			writer.close();
		}
		return 0;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new UniversityDriver(), args));
	}

}
