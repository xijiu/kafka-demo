package org.kafka.demo.admin;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class CommitOffsetTest extends AdminTest {


    @Test
    public void test() throws Exception {
        String topic = "topic_simple";
        String group = "group2";
        try (AdminClient adminClient = createAdminClient()) {
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            offsets.put(new TopicPartition(topic, 0), new OffsetAndMetadata(10));
            AlterConsumerGroupOffsetsResult offsetsResult = adminClient.alterConsumerGroupOffsets(group, offsets);
            offsetsResult.all().get();
        }
    }
}
