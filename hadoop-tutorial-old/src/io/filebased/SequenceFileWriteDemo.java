package io.filebased;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class SequenceFileWriteDemo {

	private static final String[] DATA = { "One, two, buckle my shoe", "Three, four, shut the door",
			"Five, six, pick up sticks", "Seven, eight, lay them straight", "Nine, ten, a big fat hen" };

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException {
		String uri = args[0];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(uri);
		if (fs.exists(path)) {
			System.out.printf("%s already exists. Delete it.", uri);
			fs.delete(path, false);
		}
		IntWritable key = new IntWritable();
		Text value = new Text();
		SequenceFile.Writer writer = null;
		try {
			writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass());
			for (int i = 0; i < 100; i++) {
				key.set(100 - i);
				value.set(DATA[i % DATA.length]);
				System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, value);
				writer.append(key, value);
			}
		} finally {
			IOUtils.closeStream(writer);
		}
	}

}
