package org.formation.ingest;

import java.io.BufferedInputStream;
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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.PatternSyntaxException;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.ssl.TrustStrategy;
import org.elasticsearch.client.RestClient;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.elasticsearch.client.RestClientBuilder;

import javax.net.ssl.SSLContext;

public class Ingest {

	public static void main(String[] args) throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException, URISyntaxException, KeyManagementException {
		// TODO Auto-generated method stub

		if (args.length < 2) {
			System.out.println("Usage java -jar ingest.jar <directory_to_ingest> <index> <pipeline>");
			System.exit(0);
		}
		final String pipeline = args.length >= 3 ? args[2] : "attachment";

		RestClient restClient = _buildClient();

		// Create the transport with a Jackson mapper
		ElasticsearchTransport transport = new RestClientTransport(
		    restClient, new JacksonJsonpMapper());

		// And create the API client
		ElasticsearchClient client = new ElasticsearchClient(transport);

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
					IndexResponse response = client.index(i -> i
						    .index(index)
						    .pipeline(pipeline)
						    .document(jsonMap)
						);


					System.out.println(file.getFileName() + ":" + response);
					
				} catch (PatternSyntaxException | DirectoryIteratorException | IOException e) {
					System.err.println(e);
				}
			}
		} catch (IOException x) {
			System.err.println(x);
		}

	}

	static RestClient _buildClient() throws CertificateException, IOException, KeyStoreException, NoSuchAlgorithmException, URISyntaxException, KeyManagementException {
		Properties properties = new Properties();
		try (InputStream inputStream = Ingest.class.getClassLoader().getResourceAsStream("application.properties")) {
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
