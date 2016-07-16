package hadoop.tutorial.wordcount2;

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class WordCounterTest extends WordCounter {
	private static final Path DEFAULT_INPUT;
	private static final Path DEFAULT_OUTPUT;
	private static final String[] DEFAULT_ARGS;

	static {
		DEFAULT_INPUT = new Path("./data/wordcount_input.txt");
		DEFAULT_OUTPUT = new Path("./tmp/wordcount_output2");
		DEFAULT_ARGS = new String[] { DEFAULT_INPUT.toString(), DEFAULT_OUTPUT.toString() };
	}

	@Before
	public void setUp() throws Exception {
		FileSystem fs = FileSystem.getLocal(getDefaultConf());
		fs.delete(DEFAULT_OUTPUT, true);
	}

	@After
	public void tearDown() throws Exception {
	}

	private Configuration getDefaultConf() {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "file:///");
		conf.set("mapreduce.framework.name", "local");
		conf.setInt("mapreduce.task.io.sort.mb", 1);
		return conf;
	}

	@Test
	public void testRunExit2MissingInputPath() throws Exception {
		Configuration conf = getDefaultConf();
		WordCounter counter = new WordCounter();
		counter.setConf(conf);
		assertEquals(2, counter.run(new String[] {}));
	}

	@Test
	public void testRunExit2MissingOutputPath() throws Exception {
		WordCounter counter = new WordCounter();
		counter.setConf(getDefaultConf());
		assertEquals(2, counter.run(new String[] { DEFAULT_INPUT.toString() }));
	}

	@Test
	public void testCaseSensitiveWithoutSkipPattern() throws Exception {
		Configuration conf = getDefaultConf();
		conf.setBoolean(WordCountMapper.CONF_CASE_SENSITIVE, true);
		conf.setBoolean(WordCountMapper.CONF_SKIP_PATTERN, false);

		WordCounter counter = new WordCounter();
		counter.setConf(conf);
		assertEquals(0, counter.run(DEFAULT_ARGS));
	}
	
	@Test
	public void testCaseInsensitiveWithoutSkipPattern() throws Exception {
		Configuration conf = getDefaultConf();
		conf.setBoolean(WordCountMapper.CONF_CASE_SENSITIVE, false);
		conf.setBoolean(WordCountMapper.CONF_SKIP_PATTERN, false);

		WordCounter counter = new WordCounter();
		counter.setConf(conf);
		assertEquals(0, counter.run(DEFAULT_ARGS));
	}
	
	@Test
	public void testCaseInsensitiveWithSkipPattern() throws Exception {
		Configuration conf = getDefaultConf();
		conf.setBoolean(WordCountMapper.CONF_CASE_SENSITIVE, false);
		conf.setBoolean(WordCountMapper.CONF_SKIP_PATTERN, false);

		WordCounter counter = new WordCounter();
		counter.setConf(conf);
		assertEquals(0, counter.run(DEFAULT_ARGS));
	}

}
