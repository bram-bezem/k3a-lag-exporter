package io.statnett.k3a.lagexporter;

import io.statnett.k3a.lagexporter.model.ConsumerGroupOffset;
import org.apache.kafka.common.TopicPartition;

import java.util.Set;

public record GroupOffsetResults(
    Set<ConsumerGroupOffset> consumerGroupData,
    Set<TopicPartition> topicPartitions
) {
}
