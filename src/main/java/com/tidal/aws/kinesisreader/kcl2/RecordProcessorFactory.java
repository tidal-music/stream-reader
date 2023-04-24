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

import lombok.AllArgsConstructor;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;

@AllArgsConstructor
public class RecordProcessorFactory<Payload> implements ShardRecordProcessorFactory {

    private final RecordPayloadProcessor<Payload> payloadProcessor;
    private final RecordProcessor.Configuration configuration;

    public ShardRecordProcessor shardRecordProcessor() {
        return new RecordProcessor<>(configuration, payloadProcessor);
    }
}
