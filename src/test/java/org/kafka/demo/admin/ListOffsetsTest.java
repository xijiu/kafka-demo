package org.kafka.demo.admin;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ListOffsetsTest extends AdminTest {


    @Test
    public void listOffsetsForLEO() throws Exception {
        String topic = "__consumer_offsets";
        try (AdminClient adminClient = createAdminClient()) {
            Map<String, TopicDescription> map = adminClient.describeTopics(Collections.singletonList(topic)).all().get();
            TopicDescription topicDescription = map.get(topic);
            List<TopicPartitionInfo> partitions = topicDescription.partitions();

            Map<TopicPartition, OffsetSpec> offsets = new HashMap<>();
            for (TopicPartitionInfo partitionInfo : partitions) {
                offsets.put(new TopicPartition(topic, partitionInfo.partition()), OffsetSpec.latest());
            }

            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> resultInfoMap = adminClient.listOffsets(offsets).all().get();
            for (Map.Entry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> entry : resultInfoMap.entrySet()) {
                TopicPartition topicPartition = entry.getKey();
                ListOffsetsResult.ListOffsetsResultInfo listOffsetsResultInfo = entry.getValue();
                System.out.printf("TopicPartition [%s], \tLEO [%s], \tleader epoch [%s] %n", topicPartition, listOffsetsResultInfo.offset(), listOffsetsResultInfo.leaderEpoch().orElse(-1));
            }
        }
    }
}
