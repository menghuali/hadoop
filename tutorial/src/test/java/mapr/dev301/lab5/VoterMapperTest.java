package mapr.dev301.lab5;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class VoterMapperTest extends VoterMapper {

	private MapDriver<LongWritable, Text, Text, IntWritable> driver;
	private static final LongWritable one = new LongWritable(1L);

	@Before
	public void setUp() throws Exception {
		driver = new MapDriver<>(new VoterMapper());
	}

	@After
	public void tearDown() throws Exception {
		driver = null;
	}

	@Test
	public void mapInvalidRecord_MissingFields() throws Exception {
		driver.withInput(one, new Text("0,james,14,independent,100"))
				.withCounter(VoterMapper.COUNTER_GROUP, VoterMapper.COUTNER_NAME_MISSING_FIELDS, 1).runTest();
	}

	@Test
	public void mapInvalidRecord_BadAge_NotNumber() throws Exception {
		driver.withInput(one, new Text("999997,ulysses nixon,55A,independent,819.34,27477"))
				.withCounter(VoterMapper.COUNTER_GROUP, VoterMapper.COUTNER_NAME_BAD_AGE, 1).runTest();
	}

	@Test
	public void mapInvalidRecord_BadAge_TooYoung() throws Exception {
		driver.withInput(one, new Text("999997,ulysses nixon,17,independent,819.34,27477"))
				.withCounter(VoterMapper.COUNTER_GROUP, VoterMapper.COUTNER_NAME_BAD_AGE, 1).runTest();
	}

	@Test
	public void mapInvalidRecord_BadAge_TooOld() throws Exception {
		driver.withInput(one, new Text("999997,ulysses nixon,201,independent,819.34,27477"))
				.withCounter(VoterMapper.COUNTER_GROUP, VoterMapper.COUTNER_NAME_BAD_AGE, 1).runTest();
	}

	@Test
	public void map() throws Exception {
		driver.withInput(one, new Text("0,james,20,independent,100,100"))
				.withCounter(VoterMapper.COUNTER_GROUP, VoterMapper.COUTNER_NAME_MISSING_FIELDS, 0)
				.withCounter(VoterMapper.COUNTER_GROUP, VoterMapper.COUTNER_NAME_BAD_AGE, 0)
				.withOutput(new Text("independent"), new IntWritable(20)).runTest();
	}

}
