package mapr.dev301.lab8;

import java.util.Arrays;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

public class UniversityReducer1Test extends UniversityReducer1 {

	@Test
	public void testReduceTextIterableOfIntWritableContext() throws Exception {
		new ReduceDriver<Text, IntWritable, Text, FloatWritable>(new UniversityReducer1())
				.withInput(new Text("math"),
						Arrays.asList(new IntWritable[] { new IntWritable(1), new IntWritable(2), new IntWritable(3) }))
				.withOutput(new Text("math" + MEAN_POSTFIX), new FloatWritable(2)).runTest();
	}

}
