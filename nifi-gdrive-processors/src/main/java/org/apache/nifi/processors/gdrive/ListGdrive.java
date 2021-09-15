package org.apache.nifi.processors.gdrive;

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;

public class ListGdrive extends AbstractGdriveProcessor {

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        // just get a flow file and print it? So we know it works?
    }
}
