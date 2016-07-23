package mapr.dev301.lab4;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileReader;
import java.io.LineNumberReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class UniversityDriverTest extends UniversityDriver {

	private final static Path INPUT_ROOT = new Path("./data/dev301_lab4");
	private final static Path OUTPUT_ROOT = new Path("./tmp/dev301_lab4");
	private final static String DEFAULT_PARTR_PATH = "/part-r-00000";
	private final static String KEY_VALUE_SEPARATOR = ":";
	private UniversityDriver driver;

	@Before
	public void setUp() throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "file:///");
		conf.set("mapreduce.framework.name", "local");
		conf.setInt("mapreduce.task.io.sort.mb", 1);
		conf.set("mapreduce.output.textoutputformat.separator", KEY_VALUE_SEPARATOR);

		FileSystem fs = FileSystem.getLocal(conf);
		fs.delete(OUTPUT_ROOT, true);
		fs.mkdirs(OUTPUT_ROOT);

		driver = new UniversityDriver();
		driver.setConf(conf);
	}

	@After
	public void tearDown() throws Exception {
		driver = null;
	}

	@Test
	public void testRun_1() throws Exception {
		String output = OUTPUT_ROOT + "/out1";
		driver.run(new String[] { INPUT_ROOT + "/university-1.txt", output });
		Map<String, Float> result = readOutptu(output + DEFAULT_PARTR_PATH);
		assertEquals(Float.valueOf(475.0f), result.get("satm_min"));
		assertEquals(Float.valueOf(650.0f), result.get("satm_max"));
		assertEquals(Float.valueOf(550.0f), result.get("satm_mean"));

		assertEquals(Float.valueOf(450.0f), result.get("satv_min"));
		assertEquals(Float.valueOf(625.0f), result.get("satv_max"));
		assertEquals(Float.valueOf(525.0f), result.get("satv_mean"));
	}

	@Test
	public void testRun_2() throws Exception {
		String output = OUTPUT_ROOT + "/out2";
		driver.run(new String[] { INPUT_ROOT + "/university-2.txt", output });
		Map<String, Float> result = readOutptu(output + DEFAULT_PARTR_PATH);
		assertEquals(Float.valueOf(475.0f), result.get("satm_min"));
		assertEquals(Float.valueOf(575.0f), result.get("satm_max"));
		assertEquals(Float.valueOf(525.0f), result.get("satm_mean"));

		assertEquals(Float.valueOf(450.0f), result.get("satv_min"));
		assertEquals(Float.valueOf(550.0f), result.get("satv_max"));
		assertEquals(Float.valueOf(500.0f), result.get("satv_mean"));
	}

	static Map<String, Float> readOutptu(String path) throws Exception {
		HashMap<String, Float> map = new HashMap<>();
		LineNumberReader reader = new LineNumberReader(new FileReader(new File(path)));
		try {
			String line = null;
			String[] splits = null;
			while ((line = reader.readLine()) != null) {
				splits = line.split(KEY_VALUE_SEPARATOR);
				map.put(splits[0].trim(), Float.parseFloat(splits[1].trim()));
			}
		} finally {
			reader.close();
		}
		return map;
	}

}
