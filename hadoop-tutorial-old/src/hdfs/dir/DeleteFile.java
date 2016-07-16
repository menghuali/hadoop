package hdfs.dir;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DeleteFile {

	public static void main(String[] args) {
		try {
			FileSystem fs = FileSystem.get(URI.create(args[0]), new Configuration());
			for (String path : args) {
				fs.delete(new Path(path), true);
			}
		} catch (IllegalArgumentException | IOException e) {
			e.printStackTrace();
		}
	}

}
