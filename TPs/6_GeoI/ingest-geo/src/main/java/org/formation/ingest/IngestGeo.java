package org.formation.ingest;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.PatternSyntaxException;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.ssl.TrustStrategy;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import javax.net.ssl.SSLContext;

public class IngestGeo {

	public static void main(String[] args) throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException, URISyntaxException, KeyManagementException {
		// TODO Auto-generated method stub

		if (args.length < 2) {
			System.out.println("Usage java -jar ingest.jar <dir> <index>");
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
		RestClient restClient = _buildClient();

		// Create the transport with a Jackson mapper
		ElasticsearchTransport transport = new RestClientTransport(
				restClient, new JacksonJsonpMapper());

		// And create the API client
		ElasticsearchClient client = new ElasticsearchClient(transport);

		Path dir = Paths.get(args[0]);
		String index = args[1];
		
		BufferedReader reader = null;
		try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*.*")) {
			for (Path file : stream) {
				System.out.println("File :" + file);
				reader = new BufferedReader(new FileReader(file.toFile()));
				String sCurrentLine;
				while ((sCurrentLine = reader.readLine()) != null) {
					Map<String, Object> jsonMap = new HashMap<>();
					jsonMap.put("message", sCurrentLine);

					try {
						IndexResponse response = client.index(i -> i
								.index(index)
								.pipeline("access_log")
								.document(jsonMap)
						);


						System.out.println(file.getFileName() + ":" + response);

					} catch (PatternSyntaxException | DirectoryIteratorException | IOException e) {
						System.err.println(e);
					}
				}
				System.out.println(file + ":indexed");
			}
		} catch (IOException x) {
			System.err.println(x);
		} finally {
			if (reader != null) {
				reader.close();
			}
			if ( restClient != null ) {
				restClient.close();
			}
		}


	}
	static RestClient _buildClient() throws CertificateException, IOException, KeyStoreException, NoSuchAlgorithmException, URISyntaxException, KeyManagementException {
		Properties properties = new Properties();
		try (InputStream inputStream = IngestGeo.class.getClassLoader().getResourceAsStream("application.properties")) {
			if (inputStream == null) {
				throw new IOException("Fichier non trouvé dans le classpath : ");
			}
			// Charger les propriétés depuis le fichier
			properties.load(inputStream);


			final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
			credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(
					properties.getProperty("login"), properties.getProperty("password")));
			RestClientBuilder builder;
			if (properties.getProperty("scheme").equals("https")) {
				final SSLContext sslContext = _getSSLContext();

				builder = RestClient
						.builder(new HttpHost(properties.getProperty("host"), Integer.parseInt(properties.getProperty("port")), "https"))
						.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
							@Override
							public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
								return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
										.setSSLContext(sslContext);
							}
						});
			} else {
				builder = RestClient
						.builder(new HttpHost(properties.getProperty("host"), Integer.parseInt(properties.getProperty("port")), "http"))
						.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
							@Override
							public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
								return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
							}
						});

			}
			return builder.build();
		}
	}

	private static SSLContext _getSSLContext() throws CertificateException, IOException, KeyStoreException,
			NoSuchAlgorithmException, KeyManagementException, URISyntaxException {


		// SSLContextBuilder sslBuilder = SSLContexts.custom().loadTrustMaterial(truststore, null);
		SSLContextBuilder sslBuilder = SSLContexts.custom().loadTrustMaterial(null, new TrustStrategy()
		{
			public boolean isTrusted(X509Certificate[] arg0, String arg1) throws CertificateException
			{
				return true;
			}
		});


		return sslBuilder.build();

	}

}
