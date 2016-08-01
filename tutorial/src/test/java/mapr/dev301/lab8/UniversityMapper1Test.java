package mapr.dev301.lab8;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class UniversityMapper1Test extends UniversityMapper1 {
	private static final LongWritable ONE = new LongWritable(1L);
	private MapDriver<LongWritable, Text, Text, IntWritable> driver;

	@Before
	public void setUp() throws Exception {
		driver = new MapDriver<>(new UniversityMapper1());
	}

	@After
	public void tearDown() throws Exception {
		driver = null;
	}

	@Test
	public void map() throws IOException {
		driver.withInput(ONE, new Text("10,20")).withOutput(VERBAL_KEY, new IntWritable(10))
				.withOutput(MATH_KEY, new IntWritable(20)).runTest();

	}

	@Test
	public void mapBadRecord() throws IOException {
		driver.withInput(ONE, new Text("10,20,30")).withInput(ONE, new Text("10"))
				.withCounter(CounterEnum.BAD_RECORD, 2L).runTest();

	}

	@Test
	public void mapBadVerbal() throws IOException {
		driver.withInput(ONE, new Text("10a,20")).withInput(ONE, new Text("10b,20"))
				.withCounter(CounterEnum.BAD_VERBAL, 2L).runTest();

	}

	@Test
	public void mapBadMath() throws IOException {
		driver.withInput(ONE, new Text("10,20a")).withInput(ONE, new Text("10,20b"))
				.withCounter(CounterEnum.BAD_MATH, 2L).runTest();

	}

}
