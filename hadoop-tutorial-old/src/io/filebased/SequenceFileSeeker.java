package io.filebased;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public class SequenceFileSeeker {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException {
		if (args.length < 3) {
			System.out.println("Please input the seek option [seek/sync], position and the uri.");
			System.exit(1);
		}

		String seekOpt = args[0];
		if (!"seek".equals(seekOpt) && !"sync".equals(seekOpt)) {
			System.out.println("Seek option must be either 'seek' or 'sync'.");
			System.exit(1);
		}

		long position = Long.parseLong(args[1]);
		String uri = args[2];

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path file = new Path(uri);
		if (!fs.exists(file)) {
			System.out.printf("%s does not exist. Exit.\n", uri);
			System.exit(1);
		}
		SequenceFile.Reader reader = null;
		try {
			reader = new SequenceFile.Reader(fs, file, conf);
			Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
			if ("seek".equals(seekOpt)) {
				reader.seek(position);
			} else {
				reader.sync(position);
				position = reader.getPosition();
			}
			reader.next(key, value);
			System.out.printf("[%s]\t%s\t%s\n", position, key, value);
		} finally {
			if (reader != null)
				IOUtils.closeStream(reader);
		}

	}

}
