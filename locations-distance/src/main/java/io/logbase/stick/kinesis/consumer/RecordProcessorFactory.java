package io.logbase.stick.kinesis.consumer;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;

/**
 * Used to create new record processors.
 */
public class RecordProcessorFactory implements IRecordProcessorFactory {
    /**
     * {@inheritDoc}
     */
    @Override
    public IRecordProcessor createProcessor() {
        return new RecordProcessor();
    }
}