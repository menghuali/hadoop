package hbase.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

public class RESTClientSample {

	public static void main(String[] args) throws ClientProtocolException, IOException {
		if (args == null || args.length != 2) {
			System.out.println("URL isn't found.");
			System.exit(1);
		}
		HttpClient client = new DefaultHttpClient();
		HttpGet get = new HttpGet(args[0]);
		get.setHeader("Accept", args[1]);
		HttpResponse resp = client.execute(get);
		BufferedReader reader = new BufferedReader(new InputStreamReader(resp.getEntity().getContent()));
		String line;
		while ((line = reader.readLine()) != null)
			System.out.println(line);
	}

}
