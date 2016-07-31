package hadoop.tutorial.wordcount2;

import java.io.File;
import java.net.URI;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.ClusterMapReduceTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("deprecation")
public class WordCounterTestMiniCluster extends ClusterMapReduceTestCase {

	@Before
	protected void setUp() throws Exception {
		if (System.getProperty("test.build.data") == null) {
			System.setProperty("test.build.data", "./tmp/WordCounterTestMiniCluster/data");
		}
		if (System.getProperty("hadoop.log.dir") == null) {
			System.setProperty("hadoop.log.dir", "./tmp/WordCounterTestMiniCluster/log");
		}
		File dataDir = new File("./tmp/WordCounterTestMiniCluster/data");
		if (dataDir.exists())
			FileUtils.deleteDirectory(dataDir);
		File logDir = new File("./tmp/WordCounterTestMiniCluster/log");
		if (logDir.exists())
			FileUtils.deleteDirectory(logDir);
		super.setUp();
	}

	@After
	protected void tearDown() throws Exception {
		super.tearDown();
	}

	@Test
	public void testRun_CaseSensitiveWithoutSkipPattern() throws Throwable {
		Configuration conf = createJobConf();
		conf.setBoolean(WordCountMapper.CONF_SKIP_PATTERN, false);
		Path localInput = new Path("./data/wordcount_input.txt");
		Path inputDir = getInputDir();
		Path inputFile = new Path(inputDir, "./wordcount_input.txt");
		getFileSystem().copyFromLocalFile(localInput, inputFile);
		WordCounter counter = new WordCounter();
		counter.setConf(conf);
		Path output = getOutputDir();
		int exitCode = counter.run(new String[] { inputFile.toString(), output.toString() });
		assertEquals(0, exitCode);
	}

	@Test
	public void testRun_CaseSensitiveWithSkipPattern() throws Throwable {
		Configuration conf = createJobConf();
		Path localInput = new Path("./data/wordcount_input.txt");
		Path inputDir = getInputDir();
		Path inputFile = new Path(inputDir, "wordcount_input.txt");
		getFileSystem().copyFromLocalFile(localInput, inputFile);
		WordCounter counter = new WordCounter();
		counter.setConf(conf);
		Path output = getOutputDir();
		// TBD: the following code of adding cache files does not work.
		DistributedCache.addCacheFile(
				new URI("file:///home/mli/Workspace/hadoop-tutorial-2/tutorial/wordcount2_skip_pattern.txt"), conf);
		int exitCode = counter.run(new String[] { inputFile.toString(), output.toString() });
		assertEquals(0, exitCode);
	}

}
