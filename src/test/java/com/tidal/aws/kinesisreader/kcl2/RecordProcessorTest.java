package com.tidal.aws.kinesisreader.kcl2;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

class RecordProcessorTest {

    @Test
    void config_builder_changes_default_values() {
        final RecordProcessor.Configuration config = RecordProcessor.Configuration.builder()
                .setBackoffTimeInMillis(9999)
                .setLoggingThresholdInMillis(8888)
                .setNumberOfRetries(10)
                .setDelayCheckpointing(true)
                .setCheckpointIntervalMillis(3333)
                .build();

        assertThat(config.getBackoffTimeInMillis()).isEqualTo(9999);
        assertThat(config.getLoggingThresholdInMillis()).isEqualTo(8888);
        assertThat(config.getNumberOfRetries()).isEqualTo(10);
        assertThat(config.isDelayCheckpointing()).isTrue();
        assertThat(config.getCheckpointIntervalMillis()).isEqualTo(3333);
    }

    @Test
    void config_builder_requires_non_negative_checkpoint_interval() {
        assertThatThrownBy(() -> RecordProcessor.Configuration.builder()
                .setDelayCheckpointing(true)
                .setCheckpointIntervalMillis(-1)
                .build()
        ).isInstanceOf(IllegalArgumentException.class);
    }

}