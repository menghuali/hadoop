package mapr.dev301.lab8;

import java.util.Arrays;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

public class UniversityReducer2Test extends UniversityReducer2 {

	@Test
	public void reduce() {
		new ReduceDriver<Text, FloatWritable, Text, FloatWritable>(new UniversityReducer2())
				.withInput(new Text("XY"), Arrays.asList(
						new FloatWritable[] { new FloatWritable(1), new FloatWritable(2), new FloatWritable(3) }))
				.withOutput(new Text("SUM_XY"), new FloatWritable(2));
	}

}
