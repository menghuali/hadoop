package hadoop.tutorial.wordcount2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCounter extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		if (args == null || args.length != 2) {
			System.err.printf("Usage: wordcount <in> <out> [-files <files>] [-D %s=true|false] [-D %s=true|fase]\n",
					WordCountMapper.CONF_CASE_SENSITIVE, WordCountMapper.CONF_SKIP_PATTERN);
			return 2;
		}

		Configuration conf = getConf();
		Job job = Job.getInstance(conf, WordCounter.class.getName());
		job.setJarByClass(WordCountMapper.class);
		job.setMapperClass(WordCountMapper.class);
		job.setCombinerClass(WordCountReducer.class);
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new WordCounter(), args));
	}

}
