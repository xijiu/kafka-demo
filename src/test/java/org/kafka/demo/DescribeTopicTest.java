package org.kafka.demo;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DescribeTopicTest extends AdminTest {


    @Test
    public void describeTopicTest() throws Exception {
        try (AdminClient adminClient = createAdminClient()) {
            DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Collections.singletonList("topicB"));

            Map<String, TopicDescription> topicMap = describeTopicsResult.all().get();
            for (Map.Entry<String, TopicDescription> entry : topicMap.entrySet()) {
                TopicDescription topicDescription = entry.getValue();
                List<TopicPartitionInfo> partitions = topicDescription.partitions();
                for (TopicPartitionInfo partition : partitions) {
                    System.out.println(partition);
                }
            }
        }
    }

    @Test
    public void listOffsetTest() throws Exception {
        try (AdminClient adminClient = createAdminClient()) {
            Map<TopicPartition, OffsetSpec> topicPartitionOffsets = new HashMap<>();
            TopicPartition tp = new TopicPartition("topicB", 0);
            topicPartitionOffsets.put(tp, OffsetSpec.latest());
            ListOffsetsResult listOffsetsResult = adminClient.listOffsets(topicPartitionOffsets);


            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> map = listOffsetsResult.all().get();
            ListOffsetsResult.ListOffsetsResultInfo listOffsetsResultInfo = map.get(tp);



        }
    }


}
