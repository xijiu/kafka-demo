package org.kafka.demo.admin;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 更改副本因子
 */
public class UpdateReplicaFactorTest extends AdminTest {


    @Test
    public void updateFactor() throws Exception {
        String topic = "isr_test_1";
        int partition = 0;
//        List<Integer> newReplicas = List.of(102, 101);
        List<Integer> newReplicas = List.of(102, 101, 100);
        updateFactor(topic, partition, newReplicas);
    }

    private void updateFactor(String topic, int partition, List<Integer> newReplicas) throws Exception {
        try (AdminClient adminClient = createAdminClient()) {
            Map<TopicPartition, Optional<NewPartitionReassignment>> reassignments = new HashMap<>();
            reassignments.put(new TopicPartition(topic, partition), Optional.of(new NewPartitionReassignment(newReplicas)));
            adminClient.alterPartitionReassignments(reassignments).all().get();
        }
    }
}
