package org.kafka.demo.admin;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class CreateTopicTest extends AdminTest {


    @Test
    public void test() throws Exception {
        try (AdminClient adminClient = createAdminClient()) {
            String topicName = "topic2";
            int partitionNum = 3;
            short replicaFactor = 1;
            CreateTopicsResult result = adminClient.createTopics(Collections.singletonList(new NewTopic(topicName, partitionNum, replicaFactor)));
            result.all().get();
            System.out.printf("topic %s create succeed %n", topicName);
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
