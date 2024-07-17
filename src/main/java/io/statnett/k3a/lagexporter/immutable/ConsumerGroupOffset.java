package io.statnett.k3a.lagexporter.immutable;

import org.apache.kafka.common.TopicPartition;

public record ConsumerGroupOffset(
    TopicPartition topicPartition,
    String consumerGroupId,
    long offset
) {
}
