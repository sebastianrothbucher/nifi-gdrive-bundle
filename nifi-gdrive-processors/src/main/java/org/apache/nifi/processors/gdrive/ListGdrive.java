package org.apache.nifi.processors.gdrive;

import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;

import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.util.*;

@PrimaryNodeOnly
@TriggerSerially
@TriggerWhenEmpty
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"Google", "GDrive", "List"})
@CapabilityDescription("Retrieves a listing of objects from a folder in GDrive. For each object that is listed, creates a FlowFile that represents "
        + "the object so that it can be fetched in conjunction with FetchGDrive. This Processor is designed to run on Primary Node only "
        + "in a cluster. If the primary node changes, the new Primary Node will pick up where the previous node left off without duplicating "
        + "all of the data.")
@Stateful(scopes = Scope.CLUSTER, description = "After performing a listing of keys, the timestamp of the newest key is stored, "
        + "along with the keys that share that same timestamp. This allows the Processor to list only keys that have been added or modified after "
        + "this date the next time that the Processor is run. State is stored across the cluster so that this Processor can be run on Primary Node only and if a new Primary "
        + "Node is selected, the new node can pick up where the previous node left off, without duplicating the data.")
@WritesAttributes({
        @WritesAttribute(attribute = "filename", description = "The name of the file"),
        @WritesAttribute(attribute = "fileid", description = "The id of the file"),
        @WritesAttribute(attribute = "created", description = "The created date of the file"),
        @WritesAttribute(attribute = "modified", description = "The modified date of the file"),
        @WritesAttribute(attribute = "mime.type", description = "The mime type of the file"),
        @WritesAttribute(attribute = "is.folder", description = "True if this file is a folder"),
        @WritesAttribute(attribute = "file.path", description = "The path to the file (below folder to be listed)"),
        @WritesAttribute(attribute = "parent.folder", description = "The common parent folder id = the folder to be listed"),
        @WritesAttribute(attribute = "file.parent.folder", description = "The immediate parent folder id")
})
public class ListGdrive extends AbstractGdriveProcessor {

    public static final String FOLDER_MIME_TYPE = "application/vnd.google-apps.folder";

    public static final PropertyDescriptor FOLDER = new PropertyDescriptor.Builder()
            .name("Folder")
            .displayName("Folder")
            .description("ID of the folder in GDrive")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Listing Batch Size")
            .displayName("Listing Batch Size")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .description("If not using a Record Writer, this property dictates how many objects should be listed in a single batch. Once this number is reached, the FlowFiles that have been created " +
                    "will be transferred out of the Processor. Setting this value lower may result in lower latency by sending out the FlowFiles before the complete listing has finished. However, it can " +
                    "significantly reduce performance. Larger values may take more memory to store all of the information before sending the FlowFiles out.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("100")
            .build();

    static final PropertyDescriptor FROM_BEGINNING = new PropertyDescriptor.Builder()
            .name("From beginning")
            .displayName("From beginning")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .description("List all files (true) vs list only files new since last run (false)")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor RECURSIVE_SEARCH = new PropertyDescriptor.Builder()
            .name("Search Recursively")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .description("If true, will pull files from arbitrarily nested subdirectories; otherwise, will not traverse subdirectories")
            .required(false)
            .defaultValue("false")
            .allowableValues("true", "false")
            .build();

    public static final List<PropertyDescriptor> properties = Collections.unmodifiableList(Arrays.asList(
        IAM_USER_JSON, FOLDER, BATCH_SIZE, FROM_BEGINNING, RECURSIVE_SEARCH));

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
        final boolean fromBeginning = context.getProperty(FROM_BEGINNING).asBoolean();
        long timestampPrevRun = this.currentTimestamp; // (from last run - or zero)
        try {
            final Drive service = createDriveService(context);
            getLogger().trace("Service created - start listing");
            final String rootFolderId = context.getProperty(FOLDER).evaluateAttributeExpressions().getValue();
            performListing(session, service, rootFolderId, rootFolderId, "", fromBeginning, context.getProperty(RECURSIVE_SEARCH).asBoolean(), context.getProperty(BATCH_SIZE).asInteger(), timestampPrevRun);
        } catch (final Exception e) {
            getLogger().error("Failed to list contents due to {}", new Object[] {e}, e);
            session.rollback();
            context.yield();
            return;
        }
        persistState(session);
        session.commit(); // (regardless)
    }

    private void performListing(ProcessSession session, Drive service, String folderId, String rootFolderId, String parentPath, boolean fromBeginning, boolean recursive, int batchSize, long timestampPrevRun) throws IOException {
        getLogger().trace("Pulling file infos from ", new Object[] {folderId});
        boolean first = true;
        String nextToken = null;
        long uncommitted = 0;
        final List<String[]> subfoldersToList = new LinkedList<>();
        while (first || nextToken != null) {
            first = false;
            final FileList result = service.files().list()
                    .setQ("'" + folderId + "' in parents") // also coming from NiFi
                    .setPageSize(batchSize)
                    .setPageToken(nextToken)
                    .setFields("nextPageToken, files(id, name, mimeType, createdTime, modifiedTime)")
                    .execute();
            final List<File> files = result.getFiles();
            if (null == files || files.isEmpty()) {
                getLogger().trace("No more file infos");
                break;
            }
            getLogger().trace("Pulled {} file infos", new Object[] {files.size()});
            nextToken = result.getNextPageToken();
            for (File file : files) {
                if (recursive && FOLDER_MIME_TYPE.equals(file.getMimeType())) {
                    subfoldersToList.add(new String[]{file.getId(), file.getName()});
                }
                if (file.getModifiedTime().getValue() > timestampPrevRun || fromBeginning) {
                    currentTimestamp = Math.max(currentTimestamp, file.getModifiedTime().getValue());
                    FlowFile flowFile = session.create();
                    Map<String, String> allAttributes = new HashMap<>();
                    allAttributes.put("filename", file.getName());
                    allAttributes.put("fileid", file.getId());
                    allAttributes.put("created", file.getCreatedTime().toString());
                    allAttributes.put("modified", file.getModifiedTime().toString());
                    allAttributes.put("mime.type", file.getMimeType());
                    allAttributes.put("is.folder", Boolean.toString(FOLDER_MIME_TYPE.equals(file.getMimeType())));
                    allAttributes.put("file.path", parentPath + (parentPath.length() > 0 ? "/" : "") + file.getName());
                    allAttributes.put("parent.folder", rootFolderId);
                    allAttributes.put("file.parent.folder", folderId);
                    flowFile = session.putAllAttributes(flowFile, allAttributes);
                    session.transfer(flowFile, REL_SUCCESS);
                    uncommitted++;
                }
                if (uncommitted >= batchSize) {
                    session.commit();
                    uncommitted = 0;
                }
            }
        }
        session.commit(); // (regardless)
        for (String[] subfolderToList : subfoldersToList) {
            performListing(session, service, subfolderToList[0], rootFolderId, parentPath + (parentPath.length() > 0 ? "/" : "") + subfolderToList[1], fromBeginning, recursive, batchSize, timestampPrevRun);
        }
    }
}
