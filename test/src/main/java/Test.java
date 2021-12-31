import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.FileContent;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.About;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import org.apache.commons.io.IOUtils;


public class Test {


    public static void main(String[] args) throws Exception { // TODO: could just move into NiFi anyway - or exclude some libs, i.e. run in sep. project
        final NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
        final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
        // get IAM file and provide it as stream (like we'll store it as secret in NiFi)
        Drive service = new Drive.Builder(HTTP_TRANSPORT, JSON_FACTORY, GoogleCredential
                    .fromStream(new FileInputStream("/Users/sebastianrothbucher/Downloads/nifi-331520-d5fdeed35a8d.json"))
                    .createScoped(Arrays.asList("https://www.googleapis.com/auth/drive")))
                .setApplicationName("NiFi")
                .build();
        FileList result = service.files().list()
                .setQ("'1lUN7HEFiDVNjPwYWT136XChDh5JDLoz_' in parents") // also coming from NiFi
                .setPageSize(100)
                .setFields("nextPageToken, files(id, name, mimeType, modifiedTime)")
                .execute();
        List<File> files = result.getFiles();
        if (files == null || files.isEmpty()) {
            System.out.println("No files found.");
        } else {
            System.out.println("Files:");
            for (File file : files) {
                System.out.printf("%s (%s, %s, %s, %s)\n", file.getName(), file.getMimeType(), file.getId(), file.getModifiedTime(), result.getNextPageToken());
            }
        }
        FileList specResult = service.files().list()
                .setQ("name='test.txt' and '1lUN7HEFiDVNjPwYWT136XChDh5JDLoz_' in parents") // also coming from NiFi
                .setPageSize(100)
                .setFields("nextPageToken, files(id, name, mimeType, modifiedTime)")
                .execute();
        List<File> specFiles = specResult.getFiles();
        if (specFiles == null || specFiles.isEmpty()) {
            System.out.println("No files found (spec).");
        } else {
            System.out.println("Files (spec):");
            for (File file : specFiles) {
                System.out.printf("%s (%s, %s, %s, %s)\n", file.getName(), file.getMimeType(), file.getId(), file.getModifiedTime(), specResult.getNextPageToken());
            }
        }
        System.out.println("Work one file...");
        final Drive.Files.Get get = service.files()
            .get("1Kv2RXrcQtwAmo9f66UbEJpEWvZAPGiWx")
            .setFields("id, name, mimeType, createdTime, modifiedTime");
        File fileMeta = get.execute();
        System.out.printf("%s (%s, %s, %s)\n", fileMeta.getName(), fileMeta.getMimeType(), fileMeta.getId(), fileMeta.getModifiedTime());
        String content = IOUtils.toString(new InputStreamReader(get.executeMediaAsInputStream(), "utf-8"));
        System.out.println(content);
        // (ditto a folder)
        final Drive.Files.Get folderGet = service.files()
            .get("1lUN7HEFiDVNjPwYWT136XChDh5JDLoz_")
            .setFields("owners");
        File folderMeta = folderGet.execute();
        //System.out.println(folderMeta.getOwners());
        service.files().emptyTrash(); // (srv account has sep.)
        final About.StorageQuota quota = service.about().get().setFields("storageQuota(*)").execute().getStorageQuota();
        System.out.println("Limit: " + quota.getLimit() + " - used: " + quota.getUsage());
        //System.exit(0);
        // upload and overwrite if nec - srv account has sep. quota of 15GB (ideally, target should be shared drive, not shared personal folder!)
        final String[] targetName = "newsub/photo.jpg".split("/");
        String currentFolderId = "1lUN7HEFiDVNjPwYWT136XChDh5JDLoz_";
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
                    System.out.println("Folder ID: " + folder.getId());
                    currentFolderId = folder.getId();
                } else {
                    currentFolderId = existId;
                }
            } else { // the file itself (create or overwrite)
                //fileMetadata.setOwners(folderMeta.getOwners());
                java.io.File filePath = new java.io.File("/Users/sebastianrothbucher/Desktop/photo.jpg");
                FileContent mediaContent = new FileContent("image/jpeg", filePath);
                File uploaded = null;
                if (null == existId) {
                    File fileMetadata = new File();
                    fileMetadata.setName(targetName[i]);
                    fileMetadata.setParents(Collections.singletonList(currentFolderId));
                    uploaded = service.files().create(fileMetadata, mediaContent)
                            .setFields("id")
                            .execute();
                } else {
                    uploaded = service.files().update(existId, null, mediaContent)
                            .setFields("id")
                            .execute();
                }
                System.out.println("File ID: " + uploaded.getId());
            }
        }
    }
}
