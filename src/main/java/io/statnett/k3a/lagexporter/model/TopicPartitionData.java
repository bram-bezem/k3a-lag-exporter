package io.statnett.k3a.lagexporter.model;

import org.apache.kafka.common.TopicPartition;

public record TopicPartitionData(
    TopicPartition topicPartition,
    long endOffset,
    int numReplicas
) {
}
