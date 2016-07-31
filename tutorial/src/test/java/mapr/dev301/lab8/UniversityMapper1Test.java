package mapr.dev301.lab8;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class UniversityMapper1Test extends UniversityMapper1 {
	private static final LongWritable ONE = new LongWritable(1L);
	private static final LongWritable TWO = new LongWritable(2L);
	private static final String EOL = System.getProperty("line.separator");
	private static final Text VERBAL_KEY = new Text("verbal");
	private static final Text MATH_KEY = new Text("math");

	private MapDriver<LongWritable, Text, Text, IntWritable> driver;

	@Before
	public void setUp() throws Exception {
		driver = new MapDriver<>(new UniversityMapper1());
	}

	@After
	public void tearDown() throws Exception {
		driver = null;
	}

	@Test
	public void mapBadMath() throws Exception {
		driver.withInput(ONE,
				new Text("(def-instance Adelphi" + "(state newyork)" + EOL + "(control private)" + EOL
						+ "(no-of-students thous:5-10)" + EOL + "(male:female ratio:30:70)" + EOL
						+ "(student:faculty ratio:15:1)" + EOL + "(sat verbal 500)" + EOL + "(sat math 47B)" + EOL
						+ "(expenses thous$:7-10)" + EOL + "(percent-financial-aid 60)" + EOL
						+ "(no-applicants thous:4-7)" + EOL + "(percent-admittance 70)" + EOL + "(percent-enrolled 40)"
						+ EOL + "(academics scale:1-5 2)" + EOL + "(social scale:1-5 2)" + EOL
						+ "(quality-of-life scale:1-5 2)" + EOL + "(academic-emphasis business-administration)" + EOL
						+ "(academic-emphasis biology))"))
				.withInput(TWO,
						new Text("(def-instance Adelphi" + "(state newyork)" + EOL + "(control private)" + EOL
								+ "(no-of-students thous:5-10)" + EOL + "(male:female ratio:30:70)" + EOL
								+ "(student:faculty ratio:15:1)" + EOL + "(sat verbal 500)" + EOL + "(sat math 47B)"
								+ EOL + "(expenses thous$:7-10)" + EOL + "(percent-financial-aid 60)" + EOL
								+ "(no-applicants thous:4-7)" + EOL + "(percent-admittance 70)" + EOL
								+ "(percent-enrolled 40)" + EOL + "(academics scale:1-5 2)" + EOL
								+ "(social scale:1-5 2)" + EOL + "(quality-of-life scale:1-5 2)" + EOL
								+ "(academic-emphasis business-administration)" + EOL + "(academic-emphasis biology))"))
				.withCounter(UniversityMapper1.CounterEnum.BAD_MATH, 2L).runTest();
	}

	@Test
	public void mapBadVerbal() throws Exception {
		driver.withInput(ONE,
				new Text("(def-instance Adelphi" + "(state newyork)" + EOL + "(control private)" + EOL
						+ "(no-of-students thous:5-10)" + EOL + "(male:female ratio:30:70)" + EOL
						+ "(student:faculty ratio:15:1)" + EOL + "(sat verbal 50A)" + EOL + "(sat math 475)" + EOL
						+ "(expenses thous$:7-10)" + EOL + "(percent-financial-aid 60)" + EOL
						+ "(no-applicants thous:4-7)" + EOL + "(percent-admittance 70)" + EOL + "(percent-enrolled 40)"
						+ EOL + "(academics scale:1-5 2)" + EOL + "(social scale:1-5 2)" + EOL
						+ "(quality-of-life scale:1-5 2)" + EOL + "(academic-emphasis business-administration)" + EOL
						+ "(academic-emphasis biology))"))
				.withInput(TWO,
						new Text("(def-instance Adelphi" + "(state newyork)" + EOL + "(control private)" + EOL
								+ "(no-of-students thous:5-10)" + EOL + "(male:female ratio:30:70)" + EOL
								+ "(student:faculty ratio:15:1)" + EOL + "(sat verbal 50A)" + EOL + "(sat math 475)"
								+ EOL + "(expenses thous$:7-10)" + EOL + "(percent-financial-aid 60)" + EOL
								+ "(no-applicants thous:4-7)" + EOL + "(percent-admittance 70)" + EOL
								+ "(percent-enrolled 40)" + EOL + "(academics scale:1-5 2)" + EOL
								+ "(social scale:1-5 2)" + EOL + "(quality-of-life scale:1-5 2)" + EOL
								+ "(academic-emphasis business-administration)" + EOL + "(academic-emphasis biology))"))
				.withCounter(UniversityMapper1.CounterEnum.BAD_VERBAL, 2L).runTest();
	}

	@Test
	public void mapMissingMath() throws Exception {
		driver.withInput(ONE,
				new Text("(def-instance Adelphi" + "(state newyork)" + EOL + "(control private)" + EOL
						+ "(no-of-students thous:5-10)" + EOL + "(male:female ratio:30:70)" + EOL
						+ "(student:faculty ratio:15:1)" + EOL + "(sat verbal 500)" + EOL + "(expenses thous$:7-10)"
						+ EOL + "(percent-financial-aid 60)" + EOL + "(no-applicants thous:4-7)" + EOL
						+ "(percent-admittance 70)" + EOL + "(percent-enrolled 40)" + EOL + "(academics scale:1-5 2)"
						+ EOL + "(social scale:1-5 2)" + EOL + "(quality-of-life scale:1-5 2)" + EOL
						+ "(academic-emphasis business-administration)" + EOL + "(academic-emphasis biology))"))
				.withInput(TWO,
						new Text("(def-instance Adelphi" + "(state newyork)" + EOL + "(control private)" + EOL
								+ "(no-of-students thous:5-10)" + EOL + "(male:female ratio:30:70)" + EOL
								+ "(student:faculty ratio:15:1)" + EOL + "(sat verbal 500)" + EOL
								+ "(expenses thous$:7-10)" + EOL + "(percent-financial-aid 60)" + EOL
								+ "(no-applicants thous:4-7)" + EOL + "(percent-admittance 70)" + EOL
								+ "(percent-enrolled 40)" + EOL + "(academics scale:1-5 2)" + EOL
								+ "(social scale:1-5 2)" + EOL + "(quality-of-life scale:1-5 2)" + EOL
								+ "(academic-emphasis business-administration)" + EOL + "(academic-emphasis biology))"))
				.withCounter(UniversityMapper1.CounterEnum.MISS_MATH, 2L).runTest();
	}

	@Test
	public void mapMissingVerbal() throws Exception {
		driver.withInput(ONE,
				new Text("(def-instance Adelphi" + "(state newyork)" + EOL + "(control private)" + EOL
						+ "(no-of-students thous:5-10)" + EOL + "(male:female ratio:30:70)" + EOL
						+ "(student:faculty ratio:15:1)" + EOL + "(sat math 475)" + EOL + "(expenses thous$:7-10)" + EOL
						+ "(percent-financial-aid 60)" + EOL + "(no-applicants thous:4-7)" + EOL
						+ "(percent-admittance 70)" + EOL + "(percent-enrolled 40)" + EOL + "(academics scale:1-5 2)"
						+ EOL + "(social scale:1-5 2)" + EOL + "(quality-of-life scale:1-5 2)" + EOL
						+ "(academic-emphasis business-administration)" + EOL + "(academic-emphasis biology))"))
				.withInput(TWO,
						new Text("(def-instance Adelphi" + "(state newyork)" + EOL + "(control private)" + EOL
								+ "(no-of-students thous:5-10)" + EOL + "(male:female ratio:30:70)" + EOL
								+ "(student:faculty ratio:15:1)" + EOL + "(sat math 475)" + EOL
								+ "(expenses thous$:7-10)" + EOL + "(percent-financial-aid 60)" + EOL
								+ "(no-applicants thous:4-7)" + EOL + "(percent-admittance 70)" + EOL
								+ "(percent-enrolled 40)" + EOL + "(academics scale:1-5 2)" + EOL
								+ "(social scale:1-5 2)" + EOL + "(quality-of-life scale:1-5 2)" + EOL
								+ "(academic-emphasis business-administration)" + EOL + "(academic-emphasis biology))"))
				.withCounter(UniversityMapper1.CounterEnum.MISS_VERBAL, 2L).runTest();
	}

	@Test
	public void map() throws Exception {
		driver.withInput(ONE,
				new Text("(def-instance Adelphi" + "(state newyork)" + EOL + "(control private)" + EOL
						+ "(no-of-students thous:5-10)" + EOL + "(male:female ratio:30:70)" + EOL
						+ "(student:faculty ratio:15:1)" + EOL + "(sat verbal 500)" + EOL + "(sat math 475)" + EOL
						+ "(expenses thous$:7-10)" + EOL + "(percent-financial-aid 60)" + EOL
						+ "(no-applicants thous:4-7)" + EOL + "(percent-admittance 70)" + EOL + "(percent-enrolled 40)"
						+ EOL + "(academics scale:1-5 2)" + EOL + "(social scale:1-5 2)" + EOL
						+ "(quality-of-life scale:1-5 2)" + EOL + "(academic-emphasis business-administration)" + EOL
						+ "(academic-emphasis biology))"))
				.withOutput(VERBAL_KEY, new IntWritable(500)).withOutput(MATH_KEY, new IntWritable(475)).runTest();
	}

}
