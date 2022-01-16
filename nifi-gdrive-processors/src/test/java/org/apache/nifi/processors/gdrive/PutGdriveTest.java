package org.apache.nifi.processors.gdrive;

import com.google.api.client.http.InputStreamContent;
import com.google.api.client.util.DateTime;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.Drive.Files;
import com.google.api.services.drive.Drive.Files.Create;
import com.google.api.services.drive.Drive.Files.Update;
import com.google.api.services.drive.Drive.Files.List;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PutGdriveTest {

    @Mock
    Drive driveMock;
    @Mock
    Files filesMock;
    @Mock
    Create createMock;
    @Mock
    Update updateMock;
    @Mock
    List listMock;

    DateTime createdTime = new DateTime("2021-11-11");
    DateTime modifiedTime = new DateTime("2021-12-12");
    File fileMeta = new File();
    File newFileMeta = new File();

    PutGdrive processor;
    TestRunner runner;

    @Before
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
        Mockito.when(driveMock.files()).thenReturn(filesMock);
        Mockito.when(filesMock.create(Mockito.any(File.class), Mockito.any(InputStreamContent.class))).thenAnswer((InvocationOnMock inv) -> {
            ((InputStreamContent) inv.getArgument(1)).getInputStream().close(); // avoid issues
            return createMock;
        });
        Mockito.when(createMock.setFields(Mockito.anyString())).thenReturn(createMock);
        Mockito.when(createMock.execute()).thenReturn(newFileMeta);
        Mockito.when(filesMock.update(Mockito.anyString(), Mockito.isNull(), Mockito.any(InputStreamContent.class))).thenAnswer((InvocationOnMock inv) -> {
            ((InputStreamContent) inv.getArgument(2)).getInputStream().close(); // avoid issues
            return updateMock;
        });
        Mockito.when(updateMock.setFields(Mockito.anyString())).thenReturn(updateMock);
        Mockito.when(updateMock.execute()).thenReturn(newFileMeta);
        Mockito.when(filesMock.list()).thenReturn(listMock);
        Mockito.when(listMock.setQ(Mockito.anyString())).thenReturn(listMock);
        Mockito.when(listMock.setPageSize(Mockito.anyInt())).thenReturn(listMock);
        Mockito.when(listMock.setFields(Mockito.anyString())).thenReturn(listMock);
        fileMeta.setName("x-file").setId("0815");
        newFileMeta.setName("xx-file").setId("0816").setCreatedTime(createdTime).setModifiedTime(modifiedTime);
        Mockito.when(listMock.execute()).thenReturn(new FileList().setFiles(Arrays.asList(fileMeta)));
        processor = new PutGdriveForTest();
        runner = TestRunners.newTestRunner(processor);
        runner.setProperty(PutGdrive.IAM_USER_JSON, "totally irrelevant");
        runner.setProperty(PutGdrive.FOLDER, "424242");
    }

    @Test
    public void testSuccessfulPutNew() throws Exception {
        Mockito.when(listMock.execute()).thenReturn(new FileList()); // (deviate from std)
        Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "newfile");
        attrs.put("mime.type", "test/test");
        runner.enqueue("whatever", attrs);
        runner.run();
        runner.assertTransferCount(PutGdrive.REL_SUCCESS, 1);
        runner.assertTransferCount(PutGdrive.REL_FAILURE, 0);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(PutGdrive.REL_SUCCESS).get(0);
        assertEquals("0816", flowFile.getAttribute("fileid"));
        assertEquals("true", flowFile.getAttribute("file.created"));
        Mockito.verify(createMock).execute();
        Mockito.verify(updateMock, Mockito.never()).execute();
        Mockito.verify(listMock).execute();
    }

    @Test
    public void testSuccessfulPutExistId() throws Exception {
        Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "existfile");
        attrs.put("mime.type", "test/test");
        runner.enqueue("whatever", attrs);
        runner.run();
        runner.assertTransferCount(PutGdrive.REL_SUCCESS, 1);
        runner.assertTransferCount(PutGdrive.REL_FAILURE, 0);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(PutGdrive.REL_SUCCESS).get(0);
        assertEquals("0816", flowFile.getAttribute("fileid"));
        assertEquals("false", flowFile.getAttribute("file.created"));
        Mockito.verify(createMock, Mockito.never()).execute();
        Mockito.verify(updateMock).execute();
        Mockito.verify(listMock).execute();
    }

    @Test
    public void testFailedPut() throws Exception {
        Mockito.when(updateMock.execute()).thenThrow(IOException.class); // (deviate from std)
        Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "existfile");
        attrs.put("mime.type", "test/test");
        runner.enqueue("whatever", attrs);
        runner.run();
        runner.assertTransferCount(PutGdrive.REL_SUCCESS, 0);
        runner.assertTransferCount(PutGdrive.REL_FAILURE, 1);
        assertTrue(runner.getLogger().getErrorMessages().get(0).getMsg().contains("Failed to put contents"));
    }

    class PutGdriveForTest extends PutGdrive {
        @Override
        Drive createDriveService(ProcessContext context) throws IOException, GeneralSecurityException {
            return driveMock;
        }
    }
}
