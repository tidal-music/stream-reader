// Copyright 2023 Aspiro AB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.tidal.aws.kinesisreader.kcl2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.exceptions.ThrottlingException;
import software.amazon.kinesis.lifecycle.events.*;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * The implementation of the ShardRecordProcessor interface is where the heart of the record processing logic lives.
 * In this example all we do to 'process' is log info about the records.
 */
public class RecordProcessor<Payload> implements ShardRecordProcessor {

    private static final Logger log = LoggerFactory.getLogger(RecordProcessor.class);

    private final Configuration configuration;
    private final RecordPayloadProcessor<Payload> payloadProcessor;
    private final JsonDeserializer jsonDeserializer;


    private String shardId;
    private long nextCheckpointTimeMillis;

    public RecordProcessor(
            Configuration configuration,
            RecordPayloadProcessor<Payload> payloadProcessor) {
        this.configuration = configuration;
        this.payloadProcessor = payloadProcessor;
        this.jsonDeserializer = new JsonDeserializer(payloadProcessor.objectMapper());
    }

    /**
     * Invoked by the KCL before data records are delivered to the ShardRecordProcessor instance (via
     * processRecords). In this example we do nothing except some logging.
     *
     * @param initializationInput Provides information related to initialization.
     */
    @Override
    public void initialize(InitializationInput initializationInput) {
        shardId = initializationInput.shardId();
        log.info("Initializing ShardID: {} @ Sequence: {}", shardId, initializationInput.extendedSequenceNumber());
    }

    /**
     * Handles record processing logic. The Amazon Kinesis Client Library will invoke this method to deliver
     * data records to the application. In this example we simply log our records.
     *
     * @param processRecordsInput Provides the records to be processed as well as information and capabilities
     *                            related to them (e.g. checkpointing).
     */
    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
        log.debug("Processing {} record(s)", processRecordsInput.records().size());

        processRecordsWithRetries(processRecordsInput.records());

        if (doCheckPoint()) {
            checkpoint(processRecordsInput.checkpointer());
        }

    }

    /**
     * Process records performing retries as needed. Skip "poison pill" records.
     *
     * @param records Data records to be processed.
     */
    private void processRecordsWithRetries(List<KinesisClientRecord> records) {
        for (KinesisClientRecord record : records) {
            boolean processedSuccessfully = false;
            for (int i = 0; i < configuration.getNumberOfRetries(); i++) {
                try {
                    // actually process the record
                    processRecord(record);

                    processedSuccessfully = true;
                    break;
                } catch (Throwable t) {
                    log.warn("Caught throwable while processing record " + record, t);

                    if (i < configuration.getNumberOfRetries() - 1) {
                        record.data().rewind();
                    }
                }

                // backoff if we encounter an exception.
                try {
                    Thread.sleep(configuration.getBackoffTimeInMillis());
                } catch (InterruptedException e) {
                    log.debug("Interrupted sleep", e);
                }
            }

            if (!processedSuccessfully) {
                log.error("Couldn't process record " + record + ". Skipping the record.");
            }
        }
    }

    private void processRecord(KinesisClientRecord r) {
        log.debug("Processing record: " + r);

        final Payload event = jsonDeserializer.from(
                StandardCharsets.UTF_8.decode(r.data()).toString(),
                payloadProcessor.clazz());

        runProcessor(event, r.partitionKey());
    }

    private void runProcessor(Payload event, String partitionKey) {
        final long start = System.currentTimeMillis();
        try {
            log.debug("Processing event " + event + " on shard " + shardId + " with partition key " + partitionKey);
            payloadProcessor.process(event);
        } finally {
            long totalTime = System.currentTimeMillis() - start;
            if (totalTime > configuration.getLoggingThresholdInMillis()) {
                log.warn("Processing of event [" + event + "] took more than threshold " + configuration.getLoggingThresholdInMillis() + "ms. Total time " + totalTime + "ms");
            }
        }
    }

    private boolean doCheckPoint() {
        if (!configuration.isDelayCheckpointing()) {
            return true;
        }

        // Checkpoint once every checkpoint interval.
        if (System.currentTimeMillis() > nextCheckpointTimeMillis) {
            nextCheckpointTimeMillis = System.currentTimeMillis() + configuration.getCheckpointIntervalMillis();
            return true;
        }

        return false;
    }

    /**
     * Called when the lease tied to this record processor has been lost. Once the lease has been lost,
     * the record processor can no longer checkpoint.
     *
     * @param leaseLostInput Provides access to functions and data related to the loss of the lease.
     */
    @Override
    public void leaseLost(LeaseLostInput leaseLostInput) {
        log.info("Lease lost.");
    }

    /**
     * Called when all data on this shard has been processed. Checkpointing must occur in the method for record
     * processing to be considered complete; an exception will be thrown otherwise.
     *
     * @param shardEndedInput Provides access to a checkpointer method for completing processing of the shard.
     */
    @Override
    public void shardEnded(ShardEndedInput shardEndedInput) {
        log.info("Reached shard end checkpointing.");
        checkpoint(shardEndedInput.checkpointer());
    }

    /**
     * Invoked when Scheduler has been requested to shut down (i.e. we decide to stop running the app by pressing
     * Enter). Checkpoints and logs the data a final time.
     *
     * @param shutdownRequestedInput Provides access to a checkpointer, allowing a record processor to checkpoint
     *                               before the shutdown is completed.
     */
    @Override
    public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {
        log.info("Scheduler is shutting down, checkpointing.");
        checkpoint(shutdownRequestedInput.checkpointer());
    }

    /**
     * Checkpoint with retries.
     */
    private void checkpoint(RecordProcessorCheckpointer checkpointer) {
        log.debug("Checkpointing shard " + shardId);
        for (int i = 0; i < configuration.getNumberOfRetries(); i++) {
            try {
                checkpointer.checkpoint();
                break;
            } catch (ShutdownException se) {
                // Ignore checkpoint if the processor instance has been shutdown (fail over).
                log.error("Caught shutdown exception, skipping checkpoint.", se);
                break;
            } catch (ThrottlingException e) {
                // Backoff and re-attempt checkpoint upon transient failures
                if (i >= (configuration.getNumberOfRetries() - 1)) {
                    log.error("Checkpoint failed after " + (i + 1) + "attempts.", e);
                    break;
                } else {
                    log.info("Transient issue when checkpointing - attempt " + (i + 1) + " of " + configuration.getNumberOfRetries(), e);
                }
            } catch (InvalidStateException e) {
                // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
                log.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
                break;
            }
            try {
                Thread.sleep(configuration.getBackoffTimeInMillis());
            } catch (InterruptedException e) {
                log.error("Interrupted sleep", e);
            }
        }
    }

    public static class Configuration {

        private final long backoffTimeInMillis;
        private final long loggingThresholdInMillis;
        private final int numberOfRetries;
        private final boolean delayCheckpointing;
        private final long checkpointIntervalMillis;

        public static Builder builder() {
            return new Builder();
        }

        public long getBackoffTimeInMillis() {
            return backoffTimeInMillis;
        }

        public long getLoggingThresholdInMillis() {
            return loggingThresholdInMillis;
        }

        public int getNumberOfRetries() {
            return numberOfRetries;
        }

        public boolean isDelayCheckpointing() {
            return delayCheckpointing;
        }

        public long getCheckpointIntervalMillis() {
            return checkpointIntervalMillis;
        }

        private Configuration(long backoffTimeInMillis, long loggingThresholdInMillis, int numberOfRetries, boolean delayCheckpointing, long checkpointIntervalMillis) {
            this.backoffTimeInMillis = backoffTimeInMillis;
            this.loggingThresholdInMillis = loggingThresholdInMillis;
            this.numberOfRetries = numberOfRetries;
            this.delayCheckpointing = delayCheckpointing;
            this.checkpointIntervalMillis = checkpointIntervalMillis;

            if (delayCheckpointing && checkpointIntervalMillis < 1) {
                throw new IllegalArgumentException("When delayCheckpointing is set the interval needs also to be set");
            }
        }

        public static class Builder {
            private long backoffTimeInMillis = 1000L;
            private long loggingThresholdInMillis = 5000L;
            private int numberOfRetries = 3;
            private boolean delayCheckpointing = false;
            private long checkpointIntervalMillis = -1L;


            public Builder setBackoffTimeInMillis(long backoffTimeInMillis) {
                this.backoffTimeInMillis = backoffTimeInMillis;
                return this;
            }

            public Builder setLoggingThresholdInMillis(long loggingThresholdInMillis) {
                this.loggingThresholdInMillis = loggingThresholdInMillis;
                return this;
            }

            public Builder setNumberOfRetries(int numberOfRetries) {
                this.numberOfRetries = numberOfRetries;
                return this;
            }

            public Builder setDelayCheckpointing(boolean delayCheckpointing) {
                this.delayCheckpointing = delayCheckpointing;
                return this;
            }

            public Builder setCheckpointIntervalMillis(long checkpointIntervalMillis) {
                this.checkpointIntervalMillis = checkpointIntervalMillis;
                return this;
            }

            public Configuration build() {
                return new Configuration(backoffTimeInMillis, loggingThresholdInMillis, numberOfRetries, delayCheckpointing, checkpointIntervalMillis);
            }
        }
    }
}
