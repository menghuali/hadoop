package mapr.dev301.lab4;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class UniversityReducerTest extends UniversityReducer {

	private ReduceDriver<Text, IntWritable, Text, FloatWritable> dr;

	@Before
	public void setUp() throws Exception {
		dr = new ReduceDriver<>(new UniversityReducer());
	}

	@After
	public void tearDown() throws Exception {
		dr = null;
	}

	@Test
	public void reduce() throws IOException {
		dr.withInput(new Text("satv"),
				Arrays.asList(new IntWritable[] { new IntWritable(1), new IntWritable(2), new IntWritable(3) }))
				.withOutput(new Text("satv_min"), new FloatWritable(1))
				.withOutput(new Text("satv_max"), new FloatWritable(3))
				.withOutput(new Text("satv_mean"), new FloatWritable(2)).runTest();
	}

}
