package hadoop.tutorial.wordcount2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;

public class WordCountReducerTest extends WordCountReducer {

	@Test
	public void testReduceTextIterableOfIntWritableContext() throws IOException {
		ReduceDriver<Text, IntWritable, Text, IntWritable> driver = new ReduceDriver<>();
		driver.withReducer(new WordCountReducer());
		ArrayList<IntWritable> values = new ArrayList<>();
		values.add(new IntWritable(1));
		values.add(new IntWritable(2));
		values.add(new IntWritable(3));
		driver.withInput(new Pair<Text, List<IntWritable>>(new Text("one"), values));
		driver.withOutput(new Text("one"), new IntWritable(6));
		driver.runTest();
	}

}
