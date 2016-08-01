package mapr.dev301.lab8;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class UniversityMapper2Test extends UniversityMapper2 {
	private static final LongWritable ONE = new LongWritable(1L);
	private MapDriver<LongWritable, Text, Text, FloatWritable> driver;

	@Before
	public void setUp() throws Exception {
		driver = new MapDriver<>(new UniversityMapper2());
	}

	@After
	public void tearDown() throws Exception {
		driver = null;
	}

	@Test
	public void test() throws IOException {
		Configuration conf = driver.getConfiguration();
		conf.setFloat(CONF_VERBAL_MEAN, 10);
		conf.setFloat(CONF_MATH_MEAN, 20);
		driver.withInput(ONE, new Text("12,23")).withOutput(OUT_KEY_XY, new FloatWritable(6))
				.withOutput(OUT_KEY_X2, new FloatWritable(4)).withOutput(OUT_KEY_Y2, new FloatWritable(9)).runTest();
	}

}