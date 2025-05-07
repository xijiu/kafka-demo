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

import java.util.Collection;
import java.util.Collections;
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

}
