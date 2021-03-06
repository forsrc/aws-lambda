package com.forsrc.aws.es;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.Context;

public class EsLambdaFunctionHandler implements RequestHandler<Map<String, Object>, Map<String, Object>> {

    public static final Logger LOGGER = LogManager.getLogger(EsLambdaFunctionHandler.class);
    @Override
    public Map<String, Object> handleRequest(Map<String, Object> input, Context context) {
        Map<String, Object> message = new HashMap<>();
        Map<String, Object> body = new HashMap<>();
        body.put("statusCode", 200);
        message.put("body", body);
        message.put("statusCode", 200);

        Object in = input.get("id");
        if (in == null) {
            message.put("statusCode", 500);
            body.put("errorMessage", "Parameter 'id' is null");
            body.put("statusCode", 500);
            return message;
        }

        long id = Long.parseLong(in.toString());
        LOGGER.info("--> id: {}", id);

        String queryString = "{}";
        LOGGER.info("--> query: {}", queryString);

        String endpoint = getEndpoint(id);

        try {
            CompletableFuture<Map<String, Object>> response = EsUtils.post(endpoint, queryString);
            message.put("body", response.get());
        } catch (Exception e) {
            message.put("statusCode", 500);
            body.put("errorMessage", e.getMessage());
            body.put("statusCode", 500);
        }
        return message;
    }

    private String getEndpoint(long id) {
        String esEndpoint = System.getenv("ES_ENDPOINT");
        String esIndex = System.getenv("ES_INDEX");
        String esType = System.getenv("ES_TYPE");
        String endpoint = String.format("%s/%s/%s", esEndpoint, esType, id);
        LOGGER.info("--> endpoint: {}", endpoint);
        return endpoint;
    }
}
