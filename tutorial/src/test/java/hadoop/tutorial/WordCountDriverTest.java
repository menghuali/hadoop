package hadoop.tutorial;

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import hadoop.tutorial.wordcount.WordCountDriver;

public class WordCountDriverTest extends WordCountDriver {

	@Test
	public void testRun() throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "file:///");
		conf.set("mapreduce.framework.name", "local");
		conf.setInt("mapreduce.task.io.sort.mb", 1);
		Path input = new Path("./data/wordcount_input.txt");
		Path output = new Path("./tmp/wordcount_output");

		FileSystem fs = FileSystem.getLocal(conf);
		fs.delete(output, true);

		WordCountDriver driver = new WordCountDriver();
		driver.setConf(conf);
		int exitCode = driver.run(new String[] { input.toString(), output.toString() });
		assertEquals(0, exitCode);
	}

}
