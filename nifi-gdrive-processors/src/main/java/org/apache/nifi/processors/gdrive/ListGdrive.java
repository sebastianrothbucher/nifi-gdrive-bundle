package org.apache.nifi.processors.gdrive;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;

import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

@PrimaryNodeOnly
@TriggerSerially
@TriggerWhenEmpty
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"Google", "GDrive", "list"})
@CapabilityDescription("Retrieves a listing of objects from a folder in GDrive. For each object that is listed, creates a FlowFile that represents "
        + "the object so that it can be fetched in conjunction with FetchGDrive. This Processor is designed to run on Primary Node only "
        + "in a cluster. If the primary node changes, the new Primary Node will pick up where the previous node left off without duplicating "
        + "all of the data.")
@Stateful(scopes = Scope.CLUSTER, description = "After performing a listing of keys, the timestamp of the newest key is stored, "
        + "along with the keys that share that same timestamp. This allows the Processor to list only keys that have been added or modified after "
        + "this date the next time that the Processor is run. State is stored across the cluster so that this Processor can be run on Primary Node only and if a new Primary "
        + "Node is selected, the new node can pick up where the previous node left off, without duplicating the data.")
@WritesAttributes({
        @WritesAttribute(attribute = "filename", description = "The name of the file")})
public class ListGdrive extends AbstractGdriveProcessor {

    public static final List<PropertyDescriptor> properties = Collections.unmodifiableList(Arrays.asList(
        FOLDER));

    public static final Set<Relationship> relationships = Collections.singleton(REL_SUCCESS);

    public static final String CURRENT_TIMESTAMP = "currentTimestamp";

    // State tracking
    private volatile long currentTimestamp = 0L;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    private void restoreState(final ProcessSession session) throws IOException {
        final StateMap stateMap = session.getState(Scope.CLUSTER);
        if (stateMap.getVersion() == -1L || stateMap.get(CURRENT_TIMESTAMP) == null) {
            currentTimestamp = 0L;
        } else {
            currentTimestamp = Long.parseLong(stateMap.get(CURRENT_TIMESTAMP));
        }
    }

    private void persistState(final ProcessSession session) {
        final Map<String, String> state = new HashMap<>();
        state.put(CURRENT_TIMESTAMP, String.valueOf(currentTimestamp));

        try {
            session.setState(state, Scope.CLUSTER);
        } catch (IOException ioe) {
            getLogger().error("Failed to save cluster-wide state. If NiFi is restarted, data duplication may occur", ioe);
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        try {
            restoreState(session);
        } catch (IOException ioe) {
            getLogger().error("Failed to restore processor state; yielding", ioe);
            context.yield();
            return;
        }

        // just create a flow file and print a message?!
        this.getLogger().info("ListGDrive - running!");
        FlowFile ff = session.create();
        session.putAttribute(ff, "test", "hey");
        session.putAttribute(ff, "folder", context.getProperty("folder").getValue());
        session.transfer(ff, REL_SUCCESS);
        currentTimestamp = System.currentTimeMillis();

        persistState(session);
        session.commit();
    }

    public static void main(String[] args) throws Exception { // TODO: could just move into NiFi anyway - or exclude some libs, i.e. run in sep. project
        final NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
        final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
        // get IAM file and provide it as stream (like we'll store it as secret in NiFi)
        Drive service = new Drive.Builder(HTTP_TRANSPORT, JSON_FACTORY, GoogleCredential.fromStream(new FileInputStream("/Users/sebastianrothbucher/Downloads/nifi-331520-f2655055cc78.json")))
                .setApplicationName("NiFi")
                .build();
        FileList result = service.files().list()
                .setQ("'1lUN7HEFiDVNjPwYWT136XChDh5JDLoz_' in parents") // also coming from NiFi
                .setPageSize(10)
                .setFields("nextPageToken, files(id, name)")
                .execute();
        List<File> files = result.getFiles();
        if (files == null || files.isEmpty()) {
            System.out.println("No files found.");
        } else {
            System.out.println("Files:");
            for (File file : files) {
                System.out.printf("%s (%s)\n", file.getName(), file.getId());
            }
        }
    }
}
