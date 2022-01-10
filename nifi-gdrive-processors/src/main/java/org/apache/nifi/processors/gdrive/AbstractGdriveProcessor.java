package org.apache.nifi.processors.gdrive;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.drive.Drive;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Arrays;

public abstract class AbstractGdriveProcessor extends AbstractProcessor {

    public static final String HTTPS_WWW_GOOGLEAPIS_COM_AUTH_DRIVE = "https://www.googleapis.com/auth/drive";

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("FlowFiles are routed to success relationship").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("FlowFiles are routed to failure relationship").build();

    public static final PropertyDescriptor IAM_USER_JSON = new PropertyDescriptor.Builder()
            .name("IAM JSON")
            .displayName("IAM user JSON")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();

    Drive createDriveService(ProcessContext context) throws IOException, GeneralSecurityException {
        final NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
        final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
        // get IAM file and provide it as stream (like we'll store it as secret in NiFi)
        return new Drive.Builder(HTTP_TRANSPORT, JSON_FACTORY, GoogleCredential
                .fromStream(new ByteArrayInputStream(context.getProperty(IAM_USER_JSON).evaluateAttributeExpressions().getValue().getBytes(StandardCharsets.UTF_8)))
                .createScoped(Arrays.asList(HTTPS_WWW_GOOGLEAPIS_COM_AUTH_DRIVE)))
                .setApplicationName("NiFi")
                .build();
    }
}
