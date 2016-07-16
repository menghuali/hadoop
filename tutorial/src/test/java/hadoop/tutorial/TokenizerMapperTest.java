package hadoop.tutorial;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import hadoop.tutorial.wordcount.TokenizerMapper;

public class TokenizerMapperTest extends TokenizerMapper {

	@Test
	public void testMapObjectTextContext() throws IOException {
		IntWritable one = new IntWritable(1);
		new MapDriver<Object, Text, Text, IntWritable>().withMapper(new TokenizerMapper())
				.withInput(NullWritable.get(), new Text("One Two Three")).withOutput(new Text("One"), one)
				.withOutput(new Text("Two"), one).withOutput(new Text("Three"), one).runTest();
	}

}
