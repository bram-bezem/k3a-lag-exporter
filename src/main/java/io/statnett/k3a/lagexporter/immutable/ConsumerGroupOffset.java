package io.statnett.k3a.lagexporter.immutable;

public record ConsumerGroupOffset(
    String consumerGroupId,
    long offset
) {
}
