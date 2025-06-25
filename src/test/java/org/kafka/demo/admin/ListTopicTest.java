package org.kafka.demo.admin;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.junit.Test;

import java.util.Set;

public class ListTopicTest extends AdminTest {


    @Test
    public void describeTopicTest() throws Exception {
        try (AdminClient adminClient = createAdminClient()) {

            ListTopicsResult listTopicsResult = adminClient.listTopics();
            Set<String> topics = listTopicsResult.names().get();
            for (String topic : topics) {
                System.out.println(topic);
            }
        }
    }
}
