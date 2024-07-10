package io.statnett.k3a.lagexporter.immutable;

import org.apache.kafka.common.TopicPartition;

public record TopicPartitionData(
    TopicPartition topicPartition,
    int numReplicas
) {
}
