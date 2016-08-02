package myutil;

import static org.junit.Assert.*;

import java.io.File;
import java.io.StringReader;
import java.util.Map;

import org.junit.Test;

public class MROutputReaderTest {

	@Test
	public void testReadReaderString() throws Exception {
		String lineSeparator = System.getProperty("line.separator");
		StringReader in = new StringReader("a:1" + lineSeparator + "b:2");
		Map<String, String> actual = MROutputReader.readMap(in, ":");
		assertNotNull(actual);
		assertEquals(2, actual.size());
		assertEquals("1", actual.get("a"));
		assertEquals("2", actual.get("b"));
	}

	@Test
	public void testReadFileString() throws Exception {
		File file = new File("./data/sample_mr_output.txt");
		Map<String, String> actual = MROutputReader.readMap(file, "\t");
		assertNotNull(actual);
		assertEquals(2, actual.size());
		assertEquals("32.0", actual.get("democrat_age_mean"));
		assertEquals("22.0", actual.get("democrat_age_min"));
	}

	@Test
	public void testReadFile() throws Exception {
		File file = new File("./data/sample_mr_output.txt");
		Map<String, String> actual = MROutputReader.readMap(file);
		assertNotNull(actual);
		assertEquals(2, actual.size());
		assertEquals("32.0", actual.get("democrat_age_mean"));
		assertEquals("22.0", actual.get("democrat_age_min"));
	}

}
