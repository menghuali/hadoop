package mapr.dev301.lab8;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import myutil.MROutputReader;

public class UniversityDriverTest extends UniversityDriver {

	private UniversityDriver driver;

	@Before
	public void setUp() throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "file:///");
		conf.set("mapreduce.framework.name", "local");
		conf.setInt("mapreduce.task.io.sort.mb", 30);

		FileSystem fs = FileSystem.get(conf);
		Path outputRoot = new Path("./tmp/dev301_lab8/");
		fs.delete(outputRoot, true);
		fs.mkdirs(outputRoot);

		driver = new UniversityDriver();
		driver.setConf(conf);
	}

	@After
	public void tearDown() throws Exception {
		driver = null;
	}

	@Test
	public void testRun() throws Exception {
		assertEquals(0, driver.run(new String[] { "./data/dev301_lab8/in0", "./tmp/dev301_lab8/out0",
				"./tmp/dev301_lab8/out1", "./tmp/dev301_lab8/out2", "./tmp/dev301_lab8/pcc" }));
		List<String> result = MROutputReader.readLines(new File("./tmp/dev301_lab8/pcc"));
		assertNotNull(result);
		assertEquals(1, result.size());
		assertEquals("0.18910371", result.get(0));
	}

	@Test
	public void testRunJob0() throws Exception {
		assertEquals(0, driver.runJob0("./data/dev301_lab8/in0", "./tmp/dev301_lab8/out0"));
		List<String> result = MROutputReader.readLines(new File("./tmp/dev301_lab8/out0/part-r-00000"));
		assertNotNull(result);
		assertEquals(3, result.size());
		assertTrue(result.contains("500,475"));
		assertTrue(result.contains("450,500"));
		assertTrue(result.contains("500,550"));
	}

	@Test
	public void testRunJob1() throws Exception {
		assertEquals(0, driver.runJob1("./data/dev301_lab8/out0", "./tmp/dev301_lab8/out1"));
		Map<String, String> result = MROutputReader.readMap(new File("./tmp/dev301_lab8/out1/part-r-00000"));
		assertNotNull(result);
		assertEquals("483.0", result.get("verbal_mean"));
		assertEquals("508.0", result.get("math_mean"));
	}

	@Test
	public void testRunJob2() throws Exception {
		assertEquals(0, driver.runJob2("./data/dev301_lab8/out0", "./data/dev301_lab8/out1", "./tmp/dev301_lab8/out2"));
		Map<String, String> result = MROutputReader.readMap(new File("./tmp/dev301_lab8/out2/part-r-00000"));
		assertNotNull(result);
		assertEquals("1667.0", result.get("SUM_X2"));
		assertEquals("417.0", result.get("SUM_XY"));
		assertEquals("2917.0", result.get("SUM_Y2"));
	}

	@Test
	public void testComputePCC() throws Exception {
		assertEquals(0, driver.computePCC("./data/dev301_lab8/out2", "./tmp/dev301_lab8/pcc"));
		List<String> result = MROutputReader.readLines(new File("./tmp/dev301_lab8/pcc"));
		assertNotNull(result);
		assertEquals(1, result.size());
		assertEquals("0.18910371", result.get(0));
	}

}
