package org.kafka.demo.admin;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.Test;
import org.kafka.demo.tool.CommonTools;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DescribeTopicTest extends AdminTest {


    @Test
    public void describeTopicTest() throws Exception {
        String topicName = "topic3";
        try (AdminClient adminClient = createAdminClient()) {
            DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Collections.singletonList(topicName));

            Map<String, TopicDescription> topicMap = describeTopicsResult.all().get();
            for (Map.Entry<String, TopicDescription> entry : topicMap.entrySet()) {
                String topic = entry.getKey();
                TopicDescription topicDescription = entry.getValue();
                System.out.printf("Topic name [%s], partition [%s], replica factor [%s] %n",
                        topic, topicDescription.partitions().size(), topicDescription.partitions().get(0).replicas().size());
                List<TopicPartitionInfo> partitions = topicDescription.partitions();
                for (TopicPartitionInfo partition : partitions) {
                    CommonTools.printPartitionInfo(partition);
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
