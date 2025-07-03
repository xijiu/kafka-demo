package org.kafka.demo.admin;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DescribeClusterTest extends AdminTest {


    @Test
    public void listOffsetsForLEO() throws Exception {
        String topic = "topic2";
        try (AdminClient adminClient = createAdminClient()) {
            Map<String, TopicDescription> map = adminClient.describeTopics(Collections.singletonList(topic)).all().get();
            List<TopicPartitionInfo> partitions = map.get("").partitions();
            Node leader = partitions.get(0).leader();
        }
    }


    public static void main(String[] args) {
        System.out.println(Long.MAX_VALUE + " ms");  // minute   153722867280912
        System.out.println(Long.MAX_VALUE / 1000 / 60 + " min");  // minute   153722867280912
        System.out.println(Long.MAX_VALUE / 1000 / 60 / 60 + " hour");  // hour   2562047788015
        System.out.println(Long.MAX_VALUE / 1000 / 60 / 60 / 24 + " day");  // day   106751991167
    }
}
