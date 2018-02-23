package com.forsrc.aws.es;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;

public class AwsUtils {

	public static AWSCredentials getCredentials() {
		String accessKey = System.getenv("AWS_ACCESS_KEY_ID");
		String secretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY");
		String sessionToken = System.getenv("AWS_SESSION_TOKEN");
		return new BasicSessionCredentials(accessKey, secretAccessKey, sessionToken);
	}

}
