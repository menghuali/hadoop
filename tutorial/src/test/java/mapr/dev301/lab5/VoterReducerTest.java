package mapr.dev301.lab5;

import java.util.Arrays;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class VoterReducerTest extends VoterReducer {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void reduce() {
		new ReduceDriver<Text, IntWritable, Text, FloatWritable>().withReducer(new VoterReducer())
				.withInput(new Text("republic"),
						Arrays.asList(
								new IntWritable[] { new IntWritable(20), new IntWritable(30), new IntWritable(40) }))
				.withOutput(new Text("republic_age_mean"), new FloatWritable(30))
				.withOutput(new Text("republic_age_max"), new FloatWritable(40))
				.withOutput(new Text("republic_age_min"), new FloatWritable(20));
	}

}
