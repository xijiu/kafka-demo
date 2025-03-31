package kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsResult;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.admin.ElectLeadersResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.requests.DescribeLogDirsResponse;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class AdminClientTest {
    private static String bootstrapServers = "10.253.246.12:9095,10.253.246.13:9095,10.253.246.14:9095";
    private static boolean useSASL = false;
    private static String userName = "kafka-egwiwwcls9";
    private static String password = "__CIPHER__V0uCjSXxAa1QMVNDn1fjyT46tfIq/OGDDlQ=";

//    private static String bootstrapServers = "10.253.119.253:9095,10.253.119.35:9095,10.253.119.34:9095";
//    private static boolean useSASL = true;
//    private static String userName = "kafka-dusbjr8ekm";
//    private static String password = "dusbjr8vno";


    public static void main(String[] args) throws Exception {
        long totalTime = 0;
        long totalCount = 0;

        long begin = System.currentTimeMillis();
        AdminClient adminClient = AdminClientTest.createAdminClient();
//            listTopic(adminClient);
//            showDiskUsage(adminClient);
//            electLeader(adminClient);
        deleteGroup(adminClient);
        long cost = System.currentTimeMillis() - begin;
        totalTime += cost;
        totalCount++;
        System.out.println("time cost : " + cost + " ms, avg cost is " + (totalTime / totalCount));
    }

    private static void deleteGroup(AdminClient adminClient) throws Exception {
        DeleteConsumerGroupsResult result = adminClient.deleteConsumerGroups(Collections.singleton("group1"));
        result.all().get();
    }

    private static void electLeader(AdminClient adminClient) throws Exception {
        // 查询所有的topic list
        Collection<TopicListing> topicListings = adminClient.listTopics().listings().get();

        // 获取这些topic的所有topicPartition
        List<String> topics = topicListings.stream().map(TopicListing::name).collect(Collectors.toList());
        Map<String, TopicDescription> map = adminClient.describeTopics(topics).all().get();

        // 执行选主
        Set<TopicPartition> topicPartitions = new HashSet<>();
        for (Map.Entry<String, TopicDescription> entry : map.entrySet()) {
            String topic = entry.getKey();
            List<TopicPartitionInfo> partitionInfos = entry.getValue().partitions();
            for (TopicPartitionInfo partitionInfo : partitionInfos) {
                topicPartitions.add(new TopicPartition(topic, partitionInfo.partition()));
            }
        }
        try {
            ElectLeadersResult leadersResult = adminClient.electLeaders(ElectionType.PREFERRED, topicPartitions);
            leadersResult.all().get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    private static Collection<Node> nodes = null;

    private static void showDiskUsage(AdminClient adminClient) throws Exception {
        if (nodes == null) {
            nodes = adminClient.describeCluster().nodes().get();
        }
        DescribeLogDirsResult ret = adminClient.describeLogDirs(nodes.stream().map(Node::id).collect(Collectors.toList()));
        Map<Integer, Map<String, DescribeLogDirsResponse.LogDirInfo>> brokerLogDirs = ret.all().get();
        for (Map.Entry<Integer, Map<String, DescribeLogDirsResponse.LogDirInfo>> brokerLogDir : brokerLogDirs.entrySet()) {
            Integer brokerId = brokerLogDir.getKey();
            long sum = 0;
            for (Map.Entry<String, DescribeLogDirsResponse.LogDirInfo> topicLogDir : brokerLogDir.getValue().entrySet()) {
                for (Map.Entry<TopicPartition, DescribeLogDirsResponse.ReplicaInfo> replicas : topicLogDir.getValue().replicaInfos.entrySet()) {
                    sum += replicas.getValue().size;
                }
            }
            System.out.println("brokerId " + brokerId + ": disk usage size " + sum / 1024 / 1024 / 1024 + " GB");
        }
    }

    private static void listTopic(AdminClient adminClient) throws Exception {
        ListTopicsResult listTopicsResult = adminClient.listTopics();
        Collection<TopicListing> topicListings = listTopicsResult.listings().get();
//        System.out.println("topicListings is " + topicListings);
        Set<String> topics = listTopicsResult.names().get();
        System.out.println("topic list is " + topics.size());
    }

    public static AdminClient createAdminClient() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.RETRIES_CONFIG, 3);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 20000);
        setSASL(props);
        return AdminClient.create(props);
    }

    private static void setSASL(Properties props) {
        if (useSASL) {
            props.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-256");
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            String jaasConfig = "org.apache.kafka.common.security.scram.ScramLoginModule required username='testUser' password='testPassWord';";
            props.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig.replace("testUser", userName).replace("testPassWord", password));
        }
    }
}
