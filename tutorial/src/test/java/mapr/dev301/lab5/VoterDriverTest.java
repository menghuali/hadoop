package mapr.dev301.lab5;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import myutil.MROutputReader;

public class VoterDriverTest extends VoterDriver {

	private static final Path OUTPUT_DIR = new Path("./tmp/dev301_lab5");
	private static final Path OUTPUT_FILE = new Path(OUTPUT_DIR, new Path("part-r-00000"));
	private static final String INPUT_DIR = "./data/dev301_lab5";
	private VoterDriver driver;

	@Before
	public void setUp() throws Exception {
		driver = new VoterDriver();
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "file:///");
		conf.set("mapreduce.framework.name", "local");
		conf.setInt("mapreduce.task.io.sort.mb", 1);
		driver.setConf(conf);

		FileSystem fs = FileSystem.getLocal(conf);
		fs.delete(OUTPUT_DIR, true);
	}

	@After
	public void tearDown() throws Exception {
		driver = null;
	}

	@Test
	public void run() throws Exception {
		int exitCode = driver.run(new String[] { INPUT_DIR, OUTPUT_DIR.toString() });
		assertEquals(0, exitCode);
		
		Map<String, String> result = MROutputReader.read(new File(OUTPUT_FILE.toString()));
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
