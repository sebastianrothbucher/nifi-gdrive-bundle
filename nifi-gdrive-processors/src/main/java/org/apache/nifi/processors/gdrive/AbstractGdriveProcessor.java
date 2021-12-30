package org.apache.nifi.processors.gdrive;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

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
}
