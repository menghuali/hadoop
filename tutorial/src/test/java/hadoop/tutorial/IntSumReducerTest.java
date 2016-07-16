package hadoop.tutorial;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import hadoop.tutorial.wordcount.IntSumReducer;

public class IntSumReducerTest extends IntSumReducer {

	@Test
	public void testReduceTextIterableOfIntWritableContext() throws IOException {
		ReduceDriver<Text, IntWritable, Text, IntWritable> driver = new ReduceDriver<>();
		Text key = new Text("Evening");
		driver.withReducer(new IntSumReducer()).withInput(key, Arrays.asList(new IntWritable(1), new IntWritable(2)))
				.withOutput(key, new IntWritable(3)).runTest();
	}

}
