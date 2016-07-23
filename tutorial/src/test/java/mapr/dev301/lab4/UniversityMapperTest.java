package mapr.dev301.lab4;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

public class UniversityMapperTest extends UniversityMapper {

	private static final String LINE_SEPARATOR = System.getProperty("line.separator");
	private static final String SAMPLE_RECORD = "(def-instance Adelphi" + LINE_SEPARATOR + "(state newyork)"
			+ LINE_SEPARATOR + "(control private)" + LINE_SEPARATOR + "(no-of-students thous:5-10)" + LINE_SEPARATOR
			+ "(male:female ratio:30:70)" + LINE_SEPARATOR + "(student:faculty ratio:15:1)" + LINE_SEPARATOR
			+ "(sat verbal 500)" + LINE_SEPARATOR + "(sat math 475)" + LINE_SEPARATOR + "(expenses thous$:7-10)"
			+ LINE_SEPARATOR + "(percent-financial-aid 60)" + LINE_SEPARATOR + "(no-applicants thous:4-7)"
			+ LINE_SEPARATOR + "(percent-admittance 70)" + LINE_SEPARATOR + "(percent-enrolled 40)" + LINE_SEPARATOR
			+ "(academics scale:1-5 2)" + LINE_SEPARATOR + "(social scale:1-5 2)" + LINE_SEPARATOR
			+ "(quality-of-life scale:1-5 2)" + LINE_SEPARATOR + "(academic-emphasis business-administration)"
			+ LINE_SEPARATOR + "(academic-emphasis biology))";
	private static final String SAMPLE_RECORD_NOT_SAT_VERBAL = "(def-instance Adelphi" + LINE_SEPARATOR
			+ "(state newyork)" + LINE_SEPARATOR + "(control private)" + LINE_SEPARATOR + "(no-of-students thous:5-10)"
			+ LINE_SEPARATOR + "(male:female ratio:30:70)" + LINE_SEPARATOR + "(student:faculty ratio:15:1)"
			+ LINE_SEPARATOR + "(sat math 475)" + LINE_SEPARATOR + "(expenses thous$:7-10)" + LINE_SEPARATOR
			+ "(percent-financial-aid 60)" + LINE_SEPARATOR + "(no-applicants thous:4-7)" + LINE_SEPARATOR
			+ "(percent-admittance 70)" + LINE_SEPARATOR + "(percent-enrolled 40)" + LINE_SEPARATOR
			+ "(academics scale:1-5 2)" + LINE_SEPARATOR + "(social scale:1-5 2)" + LINE_SEPARATOR
			+ "(quality-of-life scale:1-5 2)" + LINE_SEPARATOR + "(academic-emphasis business-administration)"
			+ LINE_SEPARATOR + "(academic-emphasis biology))";
	private static final String SAMPLE_RECORD_NO_SAT_MATH = "(def-instance Adelphi" + LINE_SEPARATOR + "(state newyork)"
			+ LINE_SEPARATOR + "(control private)" + LINE_SEPARATOR + "(no-of-students thous:5-10)" + LINE_SEPARATOR
			+ "(male:female ratio:30:70)" + LINE_SEPARATOR + "(student:faculty ratio:15:1)" + LINE_SEPARATOR
			+ "(sat verbal 500)" + "(expenses thous$:7-10)" + LINE_SEPARATOR + "(percent-financial-aid 60)"
			+ LINE_SEPARATOR + "(no-applicants thous:4-7)" + LINE_SEPARATOR + "(percent-admittance 70)" + LINE_SEPARATOR
			+ "(percent-enrolled 40)" + LINE_SEPARATOR + "(academics scale:1-5 2)" + LINE_SEPARATOR
			+ "(social scale:1-5 2)" + LINE_SEPARATOR + "(quality-of-life scale:1-5 2)" + LINE_SEPARATOR
			+ "(academic-emphasis business-administration)" + LINE_SEPARATOR + "(academic-emphasis biology))";

	@Test
	public void testMapLongWritableTextContext() throws Exception {
		MapDriver<LongWritable, Text, Text, IntWritable> dr = new MapDriver<>(new UniversityMapper());
		dr.withInput(new LongWritable(0), new Text(SAMPLE_RECORD)).withOutput(new Text("satv"), new IntWritable(500))
				.withOutput(new Text("satm"), new IntWritable(475)).runTest();
	}

	@Test
	public void testMapLongWritableTextContext_NoSATVerbal() throws Exception {
		MapDriver<LongWritable, Text, Text, IntWritable> dr = new MapDriver<>(new UniversityMapper());
		dr.withInput(new LongWritable(0), new Text(SAMPLE_RECORD_NOT_SAT_VERBAL)).runTest();
	}

	@Test
	public void testMapLongWritableTextContext_NoSATMath() throws Exception {
		MapDriver<LongWritable, Text, Text, IntWritable> dr = new MapDriver<>(new UniversityMapper());
		dr.withInput(new LongWritable(0), new Text(SAMPLE_RECORD_NO_SAT_MATH)).runTest();
	}

	@Test
	public void testGetIntFieldValueFromRecord_SAT_VERBAL() throws Exception {
		assertEquals(500, UniversityMapper.getIntFieldValueFromRecord(SAMPLE_RECORD, UniversityMapper.SAT_VERBAL));
	}

	@Test
	public void testGetIntFieldValueFromRecord_SAT_MATH() throws Exception {
		assertEquals(475, UniversityMapper.getIntFieldValueFromRecord(SAMPLE_RECORD, UniversityMapper.SAT_MATH));
	}

	@Test
	public void testGetIntFieldValueFromRecord_Exception() throws Exception {
		Exception exp = null;
		try {
			assertEquals(475, UniversityMapper.getIntFieldValueFromRecord(SAMPLE_RECORD, "inexist"));
		} catch (Exception e) {
			exp = e;
		}
		assertNotNull(exp);
		assertTrue(exp instanceof IllegalStateException);
		assertEquals("This record does not contain the field: inexist", exp.getMessage());
	}

}
