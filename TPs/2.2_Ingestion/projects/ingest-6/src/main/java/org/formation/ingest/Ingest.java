package org.formation.ingest;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.regex.PatternSyntaxException;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

public class Ingest {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub

		if (args.length < 2) {
			System.out.println("Usage java -jar ingest.jar <directory_to_ingest> <index> [<http_host> <http_port>]");
			System.exit(0);
		}
		String host = "localhost";
		int port = 9200;
		if (args.length >= 3) {
			host = args[2];
		}
		if (args.length == 4) {
			port = Integer.parseInt(args[3]);
		}
		RestClient restClient = RestClient.builder(new HttpHost(host, port, "http")).build();

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

				StringBuffer sbf = new StringBuffer("{\n" + "    \"name\" : \"" + file.getFileName() + "\",\n"
						+ "    \"data\" : \"" + encodedString + "\"\n}");
				HttpEntity entity = new NStringEntity(sbf.toString(), ContentType.APPLICATION_JSON);
				try {
					Request postRequest = new Request("POST", "/" + index + "/doc/?pipeline=attachment");
					postRequest.setEntity(entity);
					Response indexResponse = restClient.performRequest(postRequest);

					System.out
					.println(file.getFileName() + ":");
					System.out
					.println("\t"+ EntityUtils.toString(indexResponse.getEntity()));
					
				} catch (PatternSyntaxException | DirectoryIteratorException | IOException e) {
					System.err.println(e);
				}
			}
		} catch (IOException x) {
			System.err.println(x);
		}

		restClient.close();
	}

}
