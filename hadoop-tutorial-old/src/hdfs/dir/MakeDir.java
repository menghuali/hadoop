package hdfs.dir;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class MakeDir {

	public static void main(String[] args) {
		if (args == null || args.length == 0) {
			return;
		}
		Configuration cfg = new Configuration();
		try {
			FileSystem fs = FileSystem.get(cfg);
			for (String arg : args) {
				System.out.println("mkdir " + arg);
				fs.mkdirs(new Path(arg));
			}
		} catch (IllegalArgumentException | IOException e) {
			e.printStackTrace();
		}
	}

}
