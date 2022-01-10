package org.apache.nifi.processors.gdrive;

import com.google.api.client.http.InputStreamContent;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
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

import java.util.*;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"Google", "GDrive", "Put"})
@CapabilityDescription("Saves the contents of a flowfile to GDrive")
@ReadsAttributes({
        @ReadsAttribute(attribute = "filename", description = "Uses the FlowFile's filename as the filename for the GDrive object. Can have subfolders separated by slash i.e. folder/filename.ext"),
        @ReadsAttribute(attribute = "mime.type", description = "The mime type used for upload")
})
@WritesAttributes({
        @WritesAttribute(attribute = "fileid", description = "The id of the file"),
        @WritesAttribute(attribute = "file.created", description = "True if file was newly created, false if a new version was created for an existing file"),
        @WritesAttribute(attribute = "error.file.exists", description = "True if file exists and we fail b/c of that")
})
public class PutGdrive extends AbstractGdriveProcessor {

    public static final PropertyDescriptor FOLDER = new PropertyDescriptor.Builder()
            .name("Folder")
            .displayName("Folder")
            .description("ID of the folder in GDrive to place the file in")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor FAIL_IF_EXISTS = new PropertyDescriptor.Builder()
            .name("Fail if exists")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .description("Fail if the file already exists - otherwise overwrite = create a new version")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final List<PropertyDescriptor> properties = Collections.unmodifiableList(Arrays.asList(
        IAM_USER_JSON, FOLDER, FAIL_IF_EXISTS));

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
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException { // TODO: more trace - 2nd mode: use fileid instead of filename
        FlowFile flowFile = session.get();
        if (null == flowFile) {
            return;
        }
        try {
            final Drive service = createDriveService(context);
            final String[] targetName = flowFile.getAttribute("filename").split("/");
            String currentFolderId = context.getProperty(FOLDER).evaluateAttributeExpressions(flowFile).getValue();
            for (int i = 0; i < targetName.length; i++) {
                final boolean targetNameLast = (i == (targetName.length - 1));
                // check for existing
                String existId = null;
                FileList existResult = service.files().list()
                        .setQ("name='" + targetName[i] + "' and '" + currentFolderId + "' in parents")
                        .setPageSize(1)
                        .setFields("files(id, name)")
                        .execute();
                final List<File> existFiles = existResult.getFiles();
                if (!(existFiles == null || existFiles.isEmpty())) {
                    existId = existFiles.get(0).getId();
                }
                if (!targetNameLast) { // folder (create or just move on)
                    if (null == existId) {
                        File folderMetadata = new File();
                        folderMetadata.setName(targetName[i]);
                        folderMetadata.setMimeType("application/vnd.google-apps.folder");
                        folderMetadata.setParents(Collections.singletonList(currentFolderId));
                        File folder = service.files().create(folderMetadata)
                                .setFields("id")
                                .execute();
                        currentFolderId = folder.getId();
                    } else {
                        currentFolderId = existId;
                    }
                } else { // the file itself (create or overwrite)
                    //fileMetadata.setOwners(folderMeta.getOwners());
                    InputStreamContent mediaContent = new InputStreamContent(flowFile.getAttribute("mime.type"), session.read(flowFile));
                    File uploaded = null;
                    if (null == existId) {
                        File fileMetadata = new File();
                        fileMetadata.setName(targetName[i]);
                        fileMetadata.setParents(Collections.singletonList(currentFolderId));
                        uploaded = service.files().create(fileMetadata, mediaContent)
                                .setFields("id")
                                .execute();
                        flowFile = session.putAttribute(flowFile, "file.created", Boolean.toString(true));
                    } else if (context.getProperty(FAIL_IF_EXISTS).asBoolean()) {
                        flowFile = session.putAttribute(flowFile, "error.file.exists", Boolean.toString(true));
                        session.transfer(flowFile, REL_FAILURE);
                        session.commit();
                        context.yield();
                        return;
                    } else {
                        uploaded = service.files().update(existId, null, mediaContent)
                                .setFields("id")
                                .execute();
                        flowFile = session.putAttribute(flowFile, "file.created", Boolean.toString(false));
                    }
                    flowFile = session.putAttribute(flowFile, "fileid", uploaded.getId());
                }
            }
            session.transfer(flowFile, REL_SUCCESS);
            session.commit();
        } catch (final Exception e) {
            getLogger().error("Failed to put contents due to {}", new Object[] {e}, e);
            session.transfer(flowFile, REL_FAILURE);
            session.commit();
            context.yield();
        }
    }
}
