package org.kafka.demo.admin;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ElectLeadersResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

public class ElectLeaderTest extends AdminTest {


    @Test
    public void electPartition() throws Exception {
        String topic = "isr_test_1";
        int partition = 0;
        try (AdminClient adminClient = createAdminClient()) {
            ElectLeadersResult leadersResult = adminClient.electLeaders(ElectionType.PREFERRED, Set.of(new TopicPartition(topic, partition)));
            leadersResult.all().get();
        }
    }

    @Test
    public void electTopic() throws Exception {
        String topic = "isr_test_1";
        try (AdminClient adminClient = createAdminClient()) {
            Map<String, TopicDescription> map = adminClient.describeTopics(List.of(topic)).all().get();
            int partitionNum = map.get(topic).partitions().size();

            Set<TopicPartition> partitionSet = new HashSet<>();
            IntStream.range(0, partitionNum).forEach(partition -> partitionSet.add(new TopicPartition(topic, partition)));
            System.out.println("target partitions: " + partitionSet);
            ElectLeadersResult leadersResult = adminClient.electLeaders(ElectionType.PREFERRED, partitionSet);
            leadersResult.all().get();
        }
    }
}
