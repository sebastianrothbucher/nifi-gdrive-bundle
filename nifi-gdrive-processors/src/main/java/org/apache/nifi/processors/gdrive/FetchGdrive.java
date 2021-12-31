package org.apache.nifi.processors.gdrive;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.ByteArrayInputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"Google", "GDrive", "Get", "Fetch"})
@CapabilityDescription("Retrieves the contents from GDrive and writes it to the content of a FlowFile")
@WritesAttributes({
        @WritesAttribute(attribute = "filename", description = "The name of the file"),
        @WritesAttribute(attribute = "fileid", description = "The id of the file = the id given"),
        @WritesAttribute(attribute = "created", description = "The created date of the file"),
        @WritesAttribute(attribute = "modified", description = "The modified date of the file"),
        @WritesAttribute(attribute = "mime.type", description = "The mime type of the file")
})
public class FetchGdrive extends AbstractGdriveProcessor {

    public static final PropertyDescriptor FILE = new PropertyDescriptor.Builder()
            .name("File")
            .displayName("File")
            .description("ID of the file in GDrive")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final List<PropertyDescriptor> properties = Collections.unmodifiableList(Arrays.asList(
        IAM_USER_JSON, FILE));

    public static final Set<Relationship> relationships = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE)));

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException { // TODO: more trace - 2nd mode: use filename instead of fileid
        FlowFile flowFile = session.get();
        if (null == flowFile) {
            return;
        }
        try {
            final NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
            final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
            // get IAM file and provide it as stream (like we'll store it as secret in NiFi)
            final Drive service = new Drive.Builder(HTTP_TRANSPORT, JSON_FACTORY, GoogleCredential
                    .fromStream(new ByteArrayInputStream(context.getProperty(IAM_USER_JSON).evaluateAttributeExpressions().getValue().getBytes(StandardCharsets.UTF_8)))
                    .createScoped(Arrays.asList(HTTPS_WWW_GOOGLEAPIS_COM_AUTH_DRIVE)))
                    .setApplicationName("NiFi")
                    .build();
            final Drive.Files.Get get = service.files()
                    .get(context.getProperty(FILE).evaluateAttributeExpressions(flowFile).getValue())
                    .setFields("id, name, mimeType, createdTime, modifiedTime");
            // get metadata
            File fileMeta = get.execute();
            session.putAttribute(flowFile, "filename", fileMeta.getName());
            session.putAttribute(flowFile, "fileid", fileMeta.getId());
            session.putAttribute(flowFile, "created", fileMeta.getCreatedTime().toString());
            session.putAttribute(flowFile, "modified", fileMeta.getModifiedTime().toString());
            session.putAttribute(flowFile, "mime.type", fileMeta.getMimeType());
            // get contents
            OutputStream contentStream = session.write(flowFile);
            IOUtils.copy(get.executeMediaAsInputStream(), contentStream);
            contentStream.close();
            session.transfer(flowFile, REL_SUCCESS);
            session.commit();
        } catch (final Exception e) {
            getLogger().error("Failed to fetch contents due to {}", new Object[] {e}, e);
            session.transfer(flowFile, REL_FAILURE);
            session.commit();
            context.yield();
        }
    }
}
