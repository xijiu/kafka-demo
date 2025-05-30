package org.kafka.demo;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.DescribeLogDirsResponse;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class DiskUsageTest extends AdminTest {


    @Test
    public void describeDiskTest() throws Exception {
        try (AdminClient adminClient = createAdminClient()) {
            List<Integer> list = new ArrayList<>();
            list.add(1000);

            DescribeLogDirsResult ret = adminClient.describeLogDirs(list);

            long sum = 0;
            Map<Integer, Map<String, DescribeLogDirsResponse.LogDirInfo>> brokerLogDirs = ret.all().get();

            System.out.println("brokerLogDirs is : " + brokerLogDirs);

            for (Map.Entry<Integer, Map<String, DescribeLogDirsResponse.LogDirInfo>> brokerLogDir : brokerLogDirs.entrySet()) {
                for (Map.Entry<String, DescribeLogDirsResponse.LogDirInfo> topicLogDir : brokerLogDir.getValue().entrySet()) {
                    for (Map.Entry<TopicPartition, DescribeLogDirsResponse.ReplicaInfo> replicas : topicLogDir.getValue().replicaInfos.entrySet()) {
                        sum += replicas.getValue().size;
                    }
                }
            }

            System.out.println("sum is (Byte)" + sum);
            System.out.println("sum is (MB) " + (sum / 1024 / 1024));
            System.out.println("sum is (GB) " + (sum / 1024 / 1024 / 1024));
        }
    }


    private CompletableFuture<String> abc() {
        return CompletableFuture.failedFuture(new RuntimeException());
    }

    @Test
    public void test2() {
        CompletableFuture<String> abc = abc();
        System.out.println(abc.isCompletedExceptionally());
        abc.exceptionally(e -> {
            System.out.println("e == null " + (e instanceof RuntimeException));
            System.out.println(e.getCause().getMessage());
            return null;
        });
    }


    @Test
    public void test3() throws Exception {
        List<Thread> list = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            list.add(new Thread(() -> {
                long sum = 0;
                while (true) {
                    sum += 1;
                }
            }));
        }

        for (Thread thread : list) {
            thread.start();
        }
        for (Thread thread : list) {
            thread.join();
        }
    }

}
