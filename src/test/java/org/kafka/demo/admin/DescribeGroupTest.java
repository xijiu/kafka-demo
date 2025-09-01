package org.kafka.demo.admin;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DescribeGroupTest extends AdminTest {


    @Test
    public void describeGroupTest() throws Exception {
        String group = "group1";
        try (AdminClient adminClient = createAdminClient()) {
            Map<String, KafkaFuture<ConsumerGroupDescription>> describedGroups = adminClient.describeConsumerGroups(Collections.singleton(group)).describedGroups();
            ConsumerGroupDescription consumerGroupDescription = describedGroups.get(group).get();

            Collection<MemberDescription> members = consumerGroupDescription.members();

            System.out.println("members size: " + members.size());

            for (MemberDescription member : members) {
                System.out.println(member.clientId());
            }

        }
    }


}
