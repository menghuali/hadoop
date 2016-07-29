package mapr.dev301.lab5;

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class VoterDriverTest extends VoterDriver {

	private static final Path OUTPUT_PATH = new Path("./tmp/dev301_lab5");
	private static final String INPUT_ROOT = "./data/dev301_lab5";
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
		fs.delete(OUTPUT_PATH, true);
	}

	@After
	public void tearDown() throws Exception {
		driver = null;
	}

	@Test
	public void run() throws Exception {
		int exitCode = driver.run(new String[] { INPUT_ROOT, OUTPUT_PATH.toString() });
		assertEquals(0, exitCode);
	}

}
