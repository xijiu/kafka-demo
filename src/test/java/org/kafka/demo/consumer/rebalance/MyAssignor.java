package org.kafka.demo.consumer.rebalance;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Slf4j
public class MyAssignor extends AbstractPartitionAssignor {

    private final RangeAssignor rangeAssignor = new RangeAssignor();

    private final CooperativeStickyAssignor cooperativeStickyAssignor = new CooperativeStickyAssignor();

    @Override
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic, Map<String, Subscription> subscriptions) {
        log.info("execute MyAssignor");
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return cooperativeStickyAssignor.assign(partitionsPerTopic, subscriptions);
    }

    @Override
    public List<RebalanceProtocol> supportedProtocols() {
        return Arrays.asList(RebalanceProtocol.COOPERATIVE, RebalanceProtocol.EAGER);
    }

    @Override
    public String name() {
        return "MyAssignor";
    }
}
