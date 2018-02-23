package com.forsrc.aws.es;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import com.amazonaws.AmazonWebServiceResponse;
import com.amazonaws.http.HttpResponse;
import com.amazonaws.http.HttpResponseHandler;
import com.amazonaws.util.IOUtils;
import com.amazonaws.util.json.Jackson;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;

public class EsHttpResponseHandler implements HttpResponseHandler<AmazonWebServiceResponse<Map<String, Object>>> {

    private CompletableFuture<Map<String, Object>> completableFuture;

    public EsHttpResponseHandler(CompletableFuture<Map<String, Object>> completableFuture) {
        this.completableFuture = completableFuture;
    }

    @Override
    public AmazonWebServiceResponse<Map<String, Object>> handle(HttpResponse response) throws Exception {
        String text = IOUtils.toString(response.getContent());
        TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {
        };
        Map<String, Object> map = new ObjectMapper().readValue(text, typeRef);
        System.out.println("--> size: " + map.size());
        System.out.println("--> EsHttpResponseHandler: " + response.getStatusCode());
        System.out.println("--> EsHttpResponseHandler: " + text);
        AmazonWebServiceResponse<Map<String, Object>> awsResponse = new AmazonWebServiceResponse<>();
        awsResponse.setResult(map);
        completableFuture.complete(map);
        return awsResponse;
    }

    @Override
    public boolean needsConnectionLeftOpen() {
        return false;
    }
}
