package io.statnett.k3a.lagexporter;

import io.statnett.k3a.lagexporter.immutable.ConsumerGroupOffset;
import io.statnett.k3a.lagexporter.immutable.TopicPartitionData;
import io.statnett.k3a.lagexporter.model.ClusterData;
import io.statnett.k3a.lagexporter.utils.RegexStringListFilter;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;

public final class ClusterLagCollector {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterLagCollector.class);
    private final String clusterName;
    private final RegexStringListFilter topicFilter;
    private final RegexStringListFilter consumerGroupFilter;
    private final ClusterClient client;

    public ClusterLagCollector(final String clusterName,
                               final Collection<String> topicAllowList, final Collection<String> topicDenyList,
                               final Collection<String> consumerGroupAllowList, final Collection<String> consumerGroupDenyList,
                               final ClusterClient client) {
        this.clusterName = clusterName;
        this.topicFilter = new RegexStringListFilter(topicAllowList, topicDenyList);
        this.consumerGroupFilter = new RegexStringListFilter(consumerGroupAllowList, consumerGroupDenyList);
        this.client = client;
    }

    public ClusterData collectClusterData() {
        final boolean clientConnected = client.isConnected();
        final long startMs = System.currentTimeMillis();
        final Set<String> allConsumerGroupIds = client.consumerGroupIds(consumerGroupFilter);
        final Map<TopicPartition, List<ConsumerGroupOffset>> groupOffsetResults = findConsumerGroupOffsets(allConsumerGroupIds);
        final Set<TopicPartitionData> topicPartitionData = findReplicaCounts(groupOffsetResults.keySet());
        final Map<TopicPartition, Long> endOffsets = findEndOffsets(topicPartitionData);
        final long pollTimeMs = System.currentTimeMillis() - startMs;
        final ClusterData mutableClusterData = buildClusterData(groupOffsetResults, endOffsets, pollTimeMs);
        LOG.info("Polled lag data for {} in {} ms", clusterName, pollTimeMs);
        return mutableClusterData;
    }

    private ClusterData buildClusterData(Map<TopicPartition, List<ConsumerGroupOffset>> consumerGroupOffsets, Map<TopicPartition, Long> endOffsets, long pollTimeMs) {
        ClusterData clusterData = new ClusterData(clusterName);
        for (Map.Entry<TopicPartition, List<ConsumerGroupOffset>> entry : consumerGroupOffsets.entrySet()) {
            io.statnett.k3a.lagexporter.model.TopicPartitionData topicPartitionData = clusterData.findTopicPartitionData(entry.getKey());
            for(ConsumerGroupOffset consumerGroupOffset : entry.getValue()) {
                topicPartitionData.findConsumerGroupData(consumerGroupOffset.consumerGroupId()).setOffset(consumerGroupOffset.offset());
                topicPartitionData.calculateLags(endOffsets.get(entry.getKey()));
            }
        }
        clusterData.setPollTimeMs(pollTimeMs);
        return clusterData;
    }

    private Map<TopicPartition, List<ConsumerGroupOffset>> findConsumerGroupOffsets(final Set<String> consumerGroupIds) {
        Set<ConsumerGroupOffset> consumerGroupOffsetSet = new HashSet<>();
        client.consumerGroupOffsets(consumerGroupIds)
            .forEach((consumerGroup, offsets) -> offsets.forEach((partition, offsetAndMetadata) -> {
                final String topicName = partition.topic();
                if (!topicFilter.isAllowed(topicName)) {
                    return;
                }
                if (offsetAndMetadata == null) {
                    LOG.info("No offset data for partition {}", partition);
                    return;
                }
                consumerGroupOffsetSet.add(new ConsumerGroupOffset(partition, consumerGroup, offsetAndMetadata.offset()));
            }));
        return consumerGroupOffsetSet.stream().collect(groupingBy(ConsumerGroupOffset::topicPartition, Collectors.toUnmodifiableList()));
    }

    private Set<TopicPartitionData> findReplicaCounts(final Set<TopicPartition> topicPartitions) {
        Set<TopicPartitionData> topicPartitionData = new HashSet<>();
        Set<String> topics = topicPartitions.stream()
            .map(TopicPartition::topic)
            .collect(Collectors.toSet());
        client.describeTopics(topics).values()
            .forEach(topicDescription -> topicDescription.partitions().forEach(topicPartitionInfo -> {
                final TopicPartition topicPartition = new TopicPartition(topicDescription.name(), topicPartitionInfo.partition());
                topicPartitionData.add(new TopicPartitionData(topicPartition, topicPartitionInfo.replicas().size()));
            }));
        return topicPartitionData;
    }

    private Map<TopicPartition, Long> findEndOffsets(final Set<TopicPartitionData> topicPartitionsData) {
        if (topicPartitionsData.isEmpty()) {
            return Collections.emptyMap();
        }
        long t = System.currentTimeMillis();
        final Set<TopicPartition> topicPartitions = new HashSet<>();
        final Map<TopicPartition, Long> endOffsets = new HashMap<>();
        for (final TopicPartitionData topicPartition : topicPartitionsData) {
            topicPartitions.add(topicPartition.topicPartition());
        }
        try {
            client.endOffsets(topicPartitions)
                .forEach((partition, offset) -> {
                    endOffsets.put(partition, (offset == null ? -1 : offset));
                });
        } catch (final TimeoutException e) {
            LOG.debug("Timed out getting endOffsets");
        }
        t = System.currentTimeMillis() - t;
        LOG.debug("Found end offsets in {} ms", t);
        return endOffsets;
    }
}
