package hdfs.file.query;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

public class ListStatus {

	public static void main(String[] args) {
		if (args == null || args.length == 0)
			return;
		try {
			String uri = args[0];
			FileSystem fs = FileSystem.get(URI.create(uri), new Configuration());
			Path path = new Path(uri);
			FileStatus[] status = fs.listStatus(path);
			Path[] paths = FileUtil.stat2Paths(status);
			for (Path p : paths) {
				System.out.println(p);
			}
		} catch (IllegalArgumentException | IOException e) {
			e.printStackTrace();
		}
	}

}
