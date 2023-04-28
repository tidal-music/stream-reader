# kinesis reader

This is a KCL 2.x kinesis data stream reader (based on AWS SDK v2).

## Requirements

TODO TODO TODO TODO TODO TODO TODO TODO

* Access to create state table created by KCL 2.x see <link to aws docs>
* Read access to the kinesis stream you listen to

## Usage

TODO TODO TODO TODO TODO TODO TODO TODO

```java

import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;

class Example {

    public static class ExampleMessage {
        public String id;
        public List<Integer> data;
    }

    public static class ExampleMessageProcessor implements RecordPayloadProcessor<ExampleMessage> {

        private static final Logger log = LoggerFactory.getLogger(ExampleMessageProcessor.class);

        @Override
        public void process(ExampleMessage message) {
            log.info("Received a message from kinesis: {}", message.id);
        }

        @Override
        public Class<ExampleMessage> clazz() {
            return ExampleMessage.class;
        }
    }

    public static void main(String[] args) {
        KinesisAsyncClient kinesisAsyncClient = KinesisAsyncClient.create();
        DynamoDbAsyncClient dynamoDbAsyncClient = DynamoDbAsyncClient.create();
        CloudWatchAsyncClient cloudWatchAsyncClient = CloudWatchAsyncClient.create();

        KinesisListener<ExampleMessage> kinesisListener = new KinesisListener<>(
                streamName,
                "example_kinesis_stream_listener", // This creates a similarly named table in DDB
                InitialPositionInStream.TRIM_HORIZON,
                new RecordProcessorFactory<>(
                        new ExampleMessageProcessor(),
                        RecordProcessor.Configuration.withDefaults()),
                kinesisAsyncClient,
                dynamoDbAsyncClient,
                cloudWatchClient);

        kinesisListener.start();

        kinesisListener.shutdown();
    }
}

```