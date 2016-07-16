package hadoop.tutorial.wordcount2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

	static enum CounterEnum {
		INPUT_WORDS
	}

	final static IntWritable ONE = new IntWritable(1);
	final static String CONF_CASE_SENSITIVE = "wordcount.case.sensitive";
	final static String CONF_SKIP_PATTERN = "wordcount.skip.patterns";

	private Text word;
	private boolean caseSensitive;
	private Configuration conf;
	private Set<String> pattern2Skip;
	private BufferedReader fis;

	public WordCountMapper() {
		super();
		pattern2Skip = new HashSet<>();
		word = new Text();
	}

	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String line = caseSensitive ? value.toString() : value.toString().toLowerCase();
		for (String pattern : pattern2Skip) {
			line = line.replaceAll(pattern, "");
		}
		StringTokenizer tokenizer = new StringTokenizer(line.toString(), " \t\n\r\f,.:;\"\'");
		while (tokenizer.hasMoreTokens()) {
			word.set(tokenizer.nextToken());
			context.write(word, ONE);
			Counter counter = context.getCounter(CounterEnum.INPUT_WORDS);
			counter.increment(1);
		}
	}

	@Override
	protected void setup(Mapper<Object, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		conf = context.getConfiguration();
		caseSensitive = conf.getBoolean(CONF_CASE_SENSITIVE, true);
		if (conf.getBoolean(CONF_SKIP_PATTERN, true)) {
			URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
			if (patternsURIs != null)
				for (URI uri : patternsURIs) {
					parseSkipFile((new Path(uri)).getName());
				}
		}
	}

	private void parseSkipFile(String filename) {
		try {
			fis = new BufferedReader(new FileReader(filename));
			String pattern = null;
			while ((pattern = fis.readLine()) != null) {
				pattern2Skip.add(pattern);
			}
		} catch (Throwable e) {
			System.err.println("Caught exception while parsing the cached file '" + StringUtils.stringifyException(e));
		} finally {
			if (fis != null)
				try {
					fis.close();
				} catch (IOException e) {
					System.err.printf("Failed to close fis: %s", StringUtils.stringifyException(e));
				}
		}
	}

}
