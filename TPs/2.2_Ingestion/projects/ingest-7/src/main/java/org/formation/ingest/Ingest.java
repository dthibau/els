package org.formation.ingest;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.PatternSyntaxException;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

public class Ingest {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub

		if (args.length < 2) {
			System.out.println("Usage java -jar ingest.jar <directory_to_ingest> <index> <pipeline> [<http_host> <http_port>]");
			System.exit(0);
		}
		String pipeline = "attachment";
		String host = "localhost";
		int port = 9200;
		if (args.length >= 3) {
			pipeline = args[2];
		}
		if (args.length >= 4) {
			host = args[3];
		}
		if (args.length == 5) {
			port = Integer.parseInt(args[4]);
		}
		
		RestHighLevelClient client = new RestHighLevelClient(
				RestClient.builder(new HttpHost(host, port, "http")));

		Path dir = Paths.get(args[0]);
		String index = args[1];


		try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir,
				"*.{pdf,xls,ppt,doc,xlsx,pptx,docx,odt,ods,odp}")) {
			for (Path file : stream) {
				BufferedInputStream bin = new BufferedInputStream(Files.newInputStream(file));
				byte[] data = new byte[bin.available()];
				bin.read(data);
				bin.close();
				String encodedString = Base64.encodeBase64String(data);

		
				Map<String, Object> jsonMap = new HashMap<>();
				jsonMap.put("name", file.getFileName());
				jsonMap.put("data", encodedString);
						
				try {
					IndexRequest indexRequest = new IndexRequest(index);
					indexRequest.setPipeline(pipeline);
					indexRequest.source(jsonMap, XContentType.JSON);
					
					IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);

					System.out
					.println(file.getFileName() + ":" + indexResponse.getResult());
					
				} catch (PatternSyntaxException | DirectoryIteratorException | IOException e) {
					System.err.println(e);
				}
			}
		} catch (IOException x) {
			System.err.println(x);
		}

		client.close();
	}

}
