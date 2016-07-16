package hdfs.file.query;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class PrintFileStatus {

	public static void main(String[] args) {
		try {
			String uri = args[0];
			if (uri == null || uri.trim().length() == 0)
				return;
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(uri), conf);
			FileStatus status = fs.getFileStatus(new Path(uri));

			System.out.println("Path: " + status.getPath().toUri().getPath());
			System.out.println("Is directory: " + status.isDirectory());
			System.out.println("Is file: " + status.isFile());
			System.out.println("Modification time: " + status.getModificationTime());
			System.out.println("Replication: " + status.getReplication());
			System.out.println("Length: " + status.getLen());
			System.out.println("Block size: " + status.getBlockSize());
			System.out.println("Owner: " + status.getOwner());
			System.out.println("Group: " + status.getGroup());
			System.out.println("Permission: " + status.getPermission());
		} catch (Throwable e) {
			e.printStackTrace();
		}

	}

}
