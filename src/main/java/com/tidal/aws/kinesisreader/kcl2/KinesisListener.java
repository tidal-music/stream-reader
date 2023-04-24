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

package com.tidal.awskinesisreaderv2;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.metrics.NullMetricsFactory;
import software.amazon.kinesis.retrieval.polling.PollingConfig;

import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@Slf4j
public class KinesisListener<Payload> {

    private final Scheduler scheduler;

    public KinesisListener(
            String streamName,
            String applicationName,
            InitialPositionInStream initialPositionInStream,
            RecordProcessorFactory<Payload> recordProcessorFactory,
            KinesisAsyncClient kinesisAsyncClient,
            DynamoDbAsyncClient dynamoClient,
            CloudWatchAsyncClient cloudWatchClient) {

        ConfigsBuilder configsBuilder = new ConfigsBuilder(
                streamName,
                applicationName,
                kinesisAsyncClient,
                dynamoClient,
                cloudWatchClient,
                UUID.randomUUID().toString(),
                recordProcessorFactory);


        this.scheduler = new Scheduler(
                configsBuilder.checkpointConfig(),
                configsBuilder.coordinatorConfig(),
                configsBuilder.leaseManagementConfig()
//                        .shardSyncIntervalMillis(1000) // TODO: For tests to run reasonably fast with localstack this should be configurable.
                        .initialPositionInStream(
                                InitialPositionInStreamExtended.newInitialPosition(initialPositionInStream)),
                configsBuilder.lifecycleConfig(),
                configsBuilder.metricsConfig().metricsFactory(new NullMetricsFactory()),
                configsBuilder.processorConfig(),
                configsBuilder.retrievalConfig().retrievalSpecificConfig(
                        new PollingConfig(streamName, kinesisAsyncClient))
        );
    }

    public void start() {
        Thread schedulerThread = new Thread(scheduler);
        schedulerThread.setDaemon(true);
        schedulerThread.start();
        log.info("Started kinesis listener");
    }


    public void shutdown() {
        /**
         * Stops consuming data. Finishes processing the current batch of data already received from Kinesis
         * before shutting down.
         */
        Future<Boolean> gracefulShutdownFuture = scheduler.startGracefulShutdown();
        log.info("Waiting up to 20 seconds for shutdown to complete.");
        try {
            gracefulShutdownFuture.get(20, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("Error while trying to gracefully shutdown of kinesis stream.");
        }

        log.info("Completed, shutting down now.");
    }
}
