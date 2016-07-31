package mapr.dev301.lab5;

import java.io.File;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.ClusterMapReduceTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import myutil.MROutputReader;

public class VoterDriverMiniClusterTest extends ClusterMapReduceTestCase {

	private Path localRootPath;
	private File localRoot;

	@Before
	protected void setUp() throws Exception {
		localRootPath = new Path("./tmp/VoterDriverMiniClusterTest");
		localRoot = new File(localRootPath.toString());
		if (localRoot.exists())
			FileUtils.deleteDirectory(localRoot);

		if (System.getProperty("test.build.data") == null) {
			System.setProperty("test.build.data", "./tmp/VoterDriverMiniClusterTest/data");
		}
		if (System.getProperty("hadoop.log.dir") == null) {
			System.setProperty("hadoop.log.dir", "./tmp/VoterDriverMiniClusterTest/log");
		}
		super.setUp();
	}

	@After
	protected void tearDown() throws Exception {
		super.tearDown();
	}

	@Test
	public void testRun() throws Exception {
		FileSystem fs = getFileSystem();
		Path inputDir = getInputDir();
		fs.copyFromLocalFile(new Path("./data/dev301_lab5/myvoter.txt"), inputDir);

		Configuration conf = createJobConf();
		VoterDriver driver = new VoterDriver();
		driver.setConf(conf);
		Path outputDir = getOutputDir();
		int exitCode = driver.run(new String[] { inputDir.toString(), outputDir.toString() });
		assertEquals(0, exitCode);

		fs.copyToLocalFile(outputDir, localRootPath);

		Map<String, String> result = MROutputReader.read(new File(localRootPath.toString() + "/output/part-r-00000"));
		assertNotNull(result);
		assertEquals(6, result.size());
		assertEquals("32.0", result.get("democrat_age_mean"));
		assertEquals("22.0", result.get("democrat_age_min"));
		assertEquals("42.0", result.get("democrat_age_max"));
		assertEquals("30.0", result.get("green_age_mean"));
		assertEquals("20.0", result.get("green_age_min"));
		assertEquals("40.0", result.get("green_age_max"));
	}

}
