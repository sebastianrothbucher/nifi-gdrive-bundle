package org.apache.nifi.processors.gdrive;

import com.google.api.client.util.DateTime;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.Drive.Files;
import com.google.api.services.drive.Drive.Files.Get;
import com.google.api.services.drive.model.File;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;

import static org.junit.Assert.*;

public class FetchGdriveTest {

    @Mock
    Drive driveMock;
    @Mock
    Files filesMock;
    @Mock
    Get getMock;

    DateTime createdTime = new DateTime("2021-11-11");
    DateTime modifiedTime = new DateTime("2021-12-12");
    File fileMeta = new File();

    FetchGdrive processor;
    TestRunner runner;

    @Before
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
        Mockito.when(driveMock.files()).thenReturn(filesMock);
        Mockito.when(filesMock.get(Mockito.anyString())).thenReturn(getMock);
        Mockito.when(getMock.setFields(Mockito.anyString())).thenReturn(getMock);
        fileMeta.setName("x-file").setId("0815").setCreatedTime(createdTime).setModifiedTime(modifiedTime).setMimeType("test/bla");
        Mockito.when(getMock.execute()).thenReturn(fileMeta);
        Mockito.when(getMock.executeMediaAsInputStream()).thenReturn(new ByteArrayInputStream("TEST".getBytes(StandardCharsets.UTF_8)));
        processor = new FetchGdriveForTest();
        runner = TestRunners.newTestRunner(processor);
        runner.setProperty(FetchGdrive.IAM_USER_JSON, "totally irrelevant");
        runner.setProperty(FetchGdrive.FILE, "x-file");
    }

    @Test
    public void testSuccessfulFetch() throws Exception {
        runner.enqueue("whatever");
        runner.run();
        runner.assertTransferCount(FetchGdrive.REL_SUCCESS, 1);
        runner.assertTransferCount(FetchGdrive.REL_FAILURE, 0);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(FetchGdrive.REL_SUCCESS).get(0);
        assertEquals("TEST", flowFile.getContent());
        assertEquals("test/bla", flowFile.getAttribute("mime.type"));
        Mockito.verify(getMock).execute();
    }

    @Test
    public void testFailedFetch() throws Exception {
        Mockito.when(getMock.execute()).thenThrow(IOException.class); // (deviate from std)
        runner.enqueue("whatever");
        runner.run();
        runner.assertTransferCount(FetchGdrive.REL_SUCCESS, 0);
        runner.assertTransferCount(FetchGdrive.REL_FAILURE, 1);
        assertTrue(runner.getLogger().getErrorMessages().get(0).getMsg().contains("Failed to fetch contents"));
    }

    class FetchGdriveForTest extends FetchGdrive {
        @Override
        Drive createDriveService(ProcessContext context) throws IOException, GeneralSecurityException {
            return driveMock;
        }
    }
}
