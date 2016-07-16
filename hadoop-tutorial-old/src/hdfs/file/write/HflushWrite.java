package hdfs.file.write;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HflushWrite {

	public static void main(String[] args) {
		FSDataOutputStream out = null;
		try {
			String uri = args[0];
			FileSystem fs = FileSystem.get(URI.create(uri), new Configuration());
			Path path = new Path(uri);
			out = fs.create(path);
			out.write("Delight yourself in the LORD and he will give the desire of your heart. (Ps 37:4)"
					.getBytes("UTF-8"));
			out.hflush();
			System.out.println("File length: " + fs.getFileStatus(path).getLen());
		} catch (IllegalArgumentException | IOException e) {
			e.printStackTrace();
		} finally {
			try {
				out.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
