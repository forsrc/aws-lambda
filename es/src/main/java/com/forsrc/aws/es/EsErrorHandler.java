package com.forsrc.aws.es;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.http.HttpResponse;
import com.amazonaws.http.HttpResponseHandler;

public class EsErrorHandler implements HttpResponseHandler<AmazonServiceException> {

	@Override
	public AmazonServiceException handle(HttpResponse response) throws Exception {
		System.out.println("--> EsErrorHandler: " + response.getStatusCode());
		System.out.println("--> EsErrorHandler: " + response.getStatusText());
		AmazonServiceException ase = new AmazonServiceException("");
		ase.setStatusCode(response.getStatusCode());
		ase.setErrorCode(response.getStatusText());
		return ase;
	}

	@Override
	public boolean needsConnectionLeftOpen() {
		return false;
	}
}
