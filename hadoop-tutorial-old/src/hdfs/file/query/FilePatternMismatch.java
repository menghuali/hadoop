package hdfs.file.query;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class FilePatternMismatch {

	public static void main(String[] args) {
		try {
			FileSystem fs = FileSystem.get(URI.create(args[0]), new Configuration());
			final String regex = args[2];
			FileStatus[] status = fs.globStatus(new Path(args[1]), new PathFilter() {
				@Override
				public boolean accept(Path path) {
					return !path.getName().matches(regex);
				}
			});
			Path[] paths = FileUtil.stat2Paths(status);
			for (Path path : paths) {
				System.out.println(path);
			}
		} catch (IllegalArgumentException | IOException e) {
			e.printStackTrace();
		}
	}

}
