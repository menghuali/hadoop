package hadoop.tutorial.wordcount2;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;

public class WordCountMapperTest extends WordCountMapper {

	@Test
	public void testMapObjectTextContext_CaseSensitive_NoSkipPattern() throws IOException {

		MapDriver<Object, Text, Text, IntWritable> driver = new MapDriver<>();
		driver.withMapper(new WordCountMapper());

		driver.getConfiguration().setBoolean(WordCountMapper.CONF_CASE_SENSITIVE, true);
		driver.getConfiguration().setBoolean(WordCountMapper.CONF_SKIP_PATTERN, false);
		driver.withInput(NullWritable.get(), new Text("One one two three"));
		driver.withOutput(new Text("One"), WordCountMapper.ONE);
		driver.withOutput(new Text("one"), WordCountMapper.ONE);
		driver.withOutput(new Text("two"), WordCountMapper.ONE);
		driver.withOutput(new Text("three"), WordCountMapper.ONE);
		driver.runTest();
	}
	
	@Test
	public void testMapObjectTextContext_CaseInsensitive_NoSkipPattern() throws IOException {

		MapDriver<Object, Text, Text, IntWritable> driver = new MapDriver<>();
		driver.withMapper(new WordCountMapper());

		driver.getConfiguration().setBoolean(WordCountMapper.CONF_CASE_SENSITIVE, false);
		driver.getConfiguration().setBoolean(WordCountMapper.CONF_SKIP_PATTERN, false);
		driver.withInput(NullWritable.get(), new Text("One one two three"));
		driver.withOutput(new Text("one"), WordCountMapper.ONE);
		driver.withOutput(new Text("one"), WordCountMapper.ONE);
		driver.withOutput(new Text("two"), WordCountMapper.ONE);
		driver.withOutput(new Text("three"), WordCountMapper.ONE);
		driver.runTest();
	}
	
	@Test
	public void testMapObjectTextContext_CaseSensitive_WithSkipPattern() throws IOException {

		MapDriver<Object, Text, Text, IntWritable> driver = new MapDriver<>();
		driver.withMapper(new WordCountMapper());

		driver.getConfiguration().setBoolean(WordCountMapper.CONF_CASE_SENSITIVE, true);
		driver.getConfiguration().setBoolean(WordCountMapper.CONF_SKIP_PATTERN, true);
		driver.withInput(NullWritable.get(), new Text("One one two three"));
		ArrayList<Pair<Text, IntWritable>> expectOutputs = new ArrayList<>();
		expectOutputs.add(new Pair<Text, IntWritable>(new Text("One"), WordCountMapper.ONE));
		expectOutputs.add(new Pair<Text, IntWritable>(new Text("three"), WordCountMapper.ONE));
		driver.withAllOutput(expectOutputs);
		driver.withCacheFile("wordcount2_skip_pattern.txt");
		driver.runTest();
	}
	
	@Test
	public void testMapObjectTextContext_CaseInsensitive_WithSkipPattern() throws IOException {

		MapDriver<Object, Text, Text, IntWritable> driver = new MapDriver<>();
		driver.withMapper(new WordCountMapper());

		driver.getConfiguration().setBoolean(WordCountMapper.CONF_CASE_SENSITIVE, false);
		driver.getConfiguration().setBoolean(WordCountMapper.CONF_SKIP_PATTERN, true);
		driver.withInput(NullWritable.get(), new Text("One one two three"));
		ArrayList<Pair<Text, IntWritable>> expectOutputs = new ArrayList<>();
		expectOutputs.add(new Pair<Text, IntWritable>(new Text("three"), WordCountMapper.ONE));
		driver.withAllOutput(expectOutputs);
		driver.withCacheFile("wordcount2_skip_pattern.txt");
		driver.runTest();
	}

}
