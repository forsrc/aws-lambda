package com.forsrc.aws.es;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.DefaultRequest;
import com.amazonaws.Request;
import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.http.AmazonHttpClient;
import com.amazonaws.http.ExecutionContext;
import com.amazonaws.http.HttpMethodName;

public class EsUtils {

	public static  CompletableFuture<Map<String, Object>> post(String endpoint, String query) throws URISyntaxException {

		// String endpoint = String.format("%s/%s/%s/_count", ES_REGION, ES_INDEX,
		// ES_TYPE);

		String service = System.getenv("ES_SERVICE");
		String region = System.getenv("AWS_REGION");
		System.out.println("--> service: " + service);
		System.out.println("--> region: " + region);

		// Libraries like Jackson simplify the conversion of objects to JSON. Here, we
		// just use a string.

		// Builds the request. We need an AWS service, URI, HTTP method, and request
		// body (in this case, JSON).

		Request<?> request = new DefaultRequest<Void>(service);
		request.setEndpoint(new URI(endpoint));
		request.setHttpMethod(HttpMethodName.POST);
		Map<String, String> headers = new HashMap<>();
		headers.put("Content-Type", "application/json; charset=UTF-8");
		request.setHeaders(headers);
		request.setContent(new ByteArrayInputStream(query.getBytes()));

		// Retrieves our credentials from the computer. For more information on where
		// this class looks for credentials, see
		// http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html.

		// AWSCredentialsProvider credsProvider = new
		// DefaultAWSCredentialsProviderChain();
		// AWSCredentials creds = credsProvider.getCredentials();

		AWSCredentials creds = AwsUtils.getCredentials();

		// Signs the request using our region, service, and credentials. AWS4Signer
		// modifies the original request rather than returning a new request.

		AWS4Signer signer = new AWS4Signer();
		signer.setRegionName(region);
		signer.setServiceName(service);
		signer.sign(request, creds);

		// Creates and configures the HTTP client, creates the error and response
		// handlers, and finally executes the request.

		ClientConfiguration config = new ClientConfiguration();
		AmazonHttpClient client = new AmazonHttpClient(config);
		ExecutionContext context = new ExecutionContext(true);
		EsErrorHandler errorHandler = new EsErrorHandler();

		CompletableFuture<Map<String, Object>> completableFuture = new CompletableFuture<>();
		EsHttpResponseHandler responseHandler = new EsHttpResponseHandler(completableFuture);
		client.requestExecutionBuilder().executionContext(context).errorResponseHandler(errorHandler).request(request)
				.execute(responseHandler);
		return completableFuture;
	}
}
