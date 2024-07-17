package io.statnett.k3a.lagexporter.model;

import org.apache.kafka.common.TopicPartition;

public record ConsumerGroupData(
    TopicPartition topicPartition,
    String consumerGroupId,
    long offset,
    long lag
) {
}
