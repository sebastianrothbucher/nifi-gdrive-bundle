import java.io.FileInputStream;
import java.util.Arrays;
import java.util.List;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;


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
    }
}
