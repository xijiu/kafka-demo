package org.kafka.demo;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class CreateTopicTest extends AdminTest {


    @Test
    public void test() throws Exception {
        try (AdminClient adminClient = createAdminClient()) {
            String topicName = "create-topic-test-" + UUID.randomUUID();
            CreateTopicsResult result = adminClient.createTopics(Collections.singletonList(new NewTopic(topicName, 1, (short) 1)));
            KafkaFuture<Config> topicConfigFuture = result.config(topicName);
            Config config = topicConfigFuture.get();
            Collection<ConfigEntry> collection = config.entries();
            for (ConfigEntry configEntry : collection) {
                System.out.println("configEntry is " + configEntry);
            }


            DescribeConsumerGroupsResult describeConsumerGroupsResult = adminClient.describeConsumerGroups(null);
            describeConsumerGroupsResult.all().get().get(null);
        }
    }

    @Test
    public void batchCreateTopicsTest() throws Exception {
        try (AdminClient adminClient = createAdminClient()) {
            List<NewTopic> topics = new ArrayList<>();
            for (int i = 10; i < 1000; i++) {
                topics.add(new NewTopic("batch_create_topic_test_" + i, 1, (short) 1));
            }
            CreateTopicsResult result = adminClient.createTopics(topics);
            result.all().get();
            System.out.println("batch create topics success");
        }
    }
}
