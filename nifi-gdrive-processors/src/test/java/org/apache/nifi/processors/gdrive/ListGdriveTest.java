package org.apache.nifi.processors.gdrive;

import com.google.api.client.util.DateTime;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.Drive.Files;
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

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ListGdriveTest {

    @Mock
    Drive driveMock;
    @Mock
    Files filesMock;
    @Mock
    List listMock;

    DateTime createdTime = new DateTime("2021-11-11");
    DateTime modifiedTime = new DateTime("2021-12-12");

    File fileMeta = new File();

    ListGdrive processor;
    TestRunner runner;

    @Before
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
        Mockito.when(driveMock.files()).thenReturn(filesMock);
        Mockito.when(filesMock.list()).thenReturn(listMock);
        Mockito.when(listMock.setQ(Mockito.anyString())).thenReturn(listMock);
        Mockito.when(listMock.setPageSize(Mockito.anyInt())).thenReturn(listMock);
        Mockito.when(listMock.setPageToken(Mockito.anyString())).thenReturn(listMock);
        Mockito.when(listMock.setPageToken(Mockito.isNull())).thenReturn(listMock);
        Mockito.when(listMock.setFields(Mockito.anyString())).thenReturn(listMock);
        fileMeta.setName("x-file").setId("0815").setCreatedTime(createdTime).setModifiedTime(modifiedTime).setMimeType("test/bla");
        Mockito.when(listMock.execute()).thenReturn(new FileList().setFiles(Arrays.asList(fileMeta)));
        processor = new ListGdriveForTest();
        runner = TestRunners.newTestRunner(processor);
        runner.setProperty(ListGdrive.IAM_USER_JSON, "totally irrelevant");
        runner.setProperty(ListGdrive.FOLDER, "424242");
    }

    @Test
    public void testSuccessfulList() throws Exception {
        runner.run();
        runner.assertTransferCount(ListGdrive.REL_SUCCESS, 1);
        runner.assertTransferCount(ListGdrive.REL_FAILURE, 0);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(ListGdrive.REL_SUCCESS).get(0);
        assertEquals("x-file", flowFile.getAttribute("filename"));
        assertEquals("0815", flowFile.getAttribute("fileid"));
        assertEquals("test/bla", flowFile.getAttribute("mime.type"));
        Mockito.verify(listMock).execute();
    }

    @Test
    public void testFailedList() throws Exception {
        Mockito.when(listMock.execute()).thenThrow(IOException.class); // (deviate from std)
        runner.run();
        runner.assertTransferCount(ListGdrive.REL_SUCCESS, 0);
        runner.assertTransferCount(ListGdrive.REL_FAILURE, 0);
        assertTrue(runner.getLogger().getErrorMessages().get(0).getMsg().contains("Failed to list contents"));
    }

    class ListGdriveForTest extends ListGdrive {
        @Override
        Drive createDriveService(ProcessContext context) throws IOException, GeneralSecurityException {
            return driveMock;
        }
    }
}
