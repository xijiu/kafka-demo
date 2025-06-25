/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kafka.demo.consumer;

import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * <p>The range assignor works on a per-topic basis. For each topic, we lay out the available partitions in numeric order
 * and the consumers in lexicographic order. We then divide the number of partitions by the total number of
 * consumers to determine the number of partitions to assign to each consumer. If it does not evenly
 * divide, then the first few consumers will have one extra partition.
 *
 * <p>For example, suppose there are two consumers <code>C0</code> and <code>C1</code>, two topics <code>t0</code> and
 * <code>t1</code>, and each topic has 3 partitions, resulting in partitions <code>t0p0</code>, <code>t0p1</code>,
 * <code>t0p2</code>, <code>t1p0</code>, <code>t1p1</code>, and <code>t1p2</code>.
 *
 * <p>The assignment will be:
 * <ul>
 * <li><code>C0: [t0p0, t0p1, t1p0, t1p1]</code></li>
 * <li><code>C1: [t0p2, t1p2]</code></li>
 * </ul>
 *
 * Since the introduction of static membership, we could leverage <code>group.instance.id</code> to make the assignment behavior more sticky.
 * For the above example, after one rolling bounce, group coordinator will attempt to assign new <code>member.id</code> towards consumers,
 * for example <code>C0</code> -&gt; <code>C3</code> <code>C1</code> -&gt; <code>C2</code>.
 *
 * <p>The assignment could be completely shuffled to:
 * <ul>
 * <li><code>C3 (was C0): [t0p2, t1p2] (before was [t0p0, t0p1, t1p0, t1p1])</code>
 * <li><code>C2 (was C1): [t0p0, t0p1, t1p0, t1p1] (before was [t0p2, t1p2])</code>
 * </ul>
 *
 * The assignment change was caused by the change of <code>member.id</code> relative order, and
 * can be avoided by setting the group.instance.id.
 * Consumers will have individual instance ids <code>I1</code>, <code>I2</code>. As long as
 * 1. Number of members remain the same across generation
 * 2. Static members' identities persist across generation
 * 3. Subscription pattern doesn't change for any member
 *
 * <p>The assignment will always be:
 * <ul>
 * <li><code>I0: [t0p0, t0p1, t1p0, t1p1]</code>
 * <li><code>I1: [t0p2, t1p2]</code>
 * </ul>
 */
public class BrokerAffinityAssignor extends AbstractPartitionAssignor {
    private static final Logger log = LoggerFactory.getLogger(BrokerAffinityAssignor.class);

    public static final String AFFINITY_ASSIGNOR_NAME = "affinity";

    private Cluster metadata = null;

    @Override
    public String name() {
        return AFFINITY_ASSIGNOR_NAME;
    }

    private Map<String, List<MemberInfo>> consumersPerTopic(Map<String, Subscription> consumerMetadata) {
        Map<String, List<MemberInfo>> topicToConsumers = new HashMap<>();
        for (Map.Entry<String, Subscription> subscriptionEntry : consumerMetadata.entrySet()) {
            String consumerId = subscriptionEntry.getKey();
            MemberInfo memberInfo = new MemberInfo(consumerId, subscriptionEntry.getValue().groupInstanceId());
            for (String topic : subscriptionEntry.getValue().topics()) {
                put(topicToConsumers, topic, memberInfo);
            }
        }
        log.info("execute range assignor, consumerMetadata {}, topicToConsumers {}",
                consumerMetadata, topicToConsumers);
        return topicToConsumers;
    }

    @Override
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
                                                    Map<String, Subscription> subscriptions) {
        // 打印非法的consumer id
        maybePrintInvalidConsumerId(subscriptions);
        Map<String, List<MemberInfo>> consumersPerTopic = consumersPerTopic(subscriptions);

        Map<String, List<TopicPartition>> assignment = new HashMap<>();
        for (String memberId : subscriptions.keySet()) {
            assignment.put(memberId, new ArrayList<>());
        }

        for (Map.Entry<String, List<MemberInfo>> topicEntry : consumersPerTopic.entrySet()) {
            String topic = topicEntry.getKey();
            List<MemberInfo> consumersForTopic = topicEntry.getValue();

            Integer numPartitionsForTopic = partitionsPerTopic.get(topic);
            if (numPartitionsForTopic == null) {
                continue;
            }
            assignForSingleTopic(topic, numPartitionsForTopic, consumersForTopic, assignment);
        }

        log.info("execute range assignor, partitionsPerTopic {}, consumer number {}, detail {}, " +
                        "topic-partition leader {}, final assignment {}",
                partitionsPerTopic, subscriptions.size(), subscriptions,
                assembleAllTopicMetadata(partitionsPerTopic.keySet()), assignment);
        return assignment;
    }

    /**
     * consumerId需要满足以下格式：
     * consumer-192.168.1.1-affinity-9ddae128-37b5-4158-8d99-5fea5e1027e0
     *
     * @param subscriptions consumer列表
     */
    private void maybePrintInvalidConsumerId(Map<String, Subscription> subscriptions) {
        for (Map.Entry<String, Subscription> subscriptionEntry : subscriptions.entrySet()) {
            String consumerId = subscriptionEntry.getKey();
            if (!consumerId.contains("consumer") || !consumerId.contains("affinity")) {
                log.warn("consumer id is invalid, invalid consumerId {}", consumerId);
            }
        }
    }

    private void assignForSingleTopic(String topic, int partitionNum, List<MemberInfo> consumerList,
                                      Map<String, List<TopicPartition>> resultAssignmentMap) {
        // 没有能被亲和的partition集合
        Set<TopicPartition> nonAffinity = new HashSet<>();

        // 对可亲和的consumer进行分配
        for (int partition = 0; partition < partitionNum; partition++) {
            TopicPartition topicPartition = new TopicPartition(topic, partition);
            String brokerIp = findPartitionLeaderBrokerIp(topicPartition);
            // 是否可以亲和
            boolean hitAffinity = false;
            if (brokerIp != null && !brokerIp.isEmpty()) {
                List<MemberInfo> memberInfos = findMemberInfoByIp(consumerList, brokerIp);
                if (!memberInfos.isEmpty()) {
                    // 找到这些member中分配了partition最少的那个
                    MemberInfo memberInfo = findMinPartitionMember(memberInfos, resultAssignmentMap);
                    resultAssignmentMap.get(memberInfo.memberId).add(topicPartition);
                    hitAffinity = true;
                }
            }
            // 找不到亲和，那么记录在集合中
            if (!hitAffinity) {
                nonAffinity.add(topicPartition);
            }
        }
        // 找不到亲和的partition的分配
        assignNonAffinity(nonAffinity, resultAssignmentMap);
    }

    /**
     * 寻找当前consumer list中分配了partition最少的那个
     *
     * @param memberInfos   consumer列表
     * @param resultAssignmentMap   最终的分配方案结果
     * @return  partition中最小的member
     */
    private MemberInfo findMinPartitionMember(List<MemberInfo> memberInfos,
                                              Map<String, List<TopicPartition>> resultAssignmentMap) {
        MemberInfo resultMember = null;
        for (MemberInfo memberInfo : memberInfos) {
            if (resultMember == null) {
                resultMember = memberInfo;
            } else {
                int tmpSize = resultAssignmentMap.get(memberInfo.memberId).size();
                int minSize = resultAssignmentMap.get(resultMember.memberId).size();
                if (tmpSize < minSize) {
                    resultMember = memberInfo;
                }
            }
        }
        return resultMember;
    }

    /**
     * 找不到亲和的partition分配consumer
     *
     * @param nonAffinitySet   找不到亲和的partition集合
     * @param resultAssignmentMap   分配结果map
     */
    private void assignNonAffinity(Set<TopicPartition> nonAffinitySet,
                                   Map<String, List<TopicPartition>> resultAssignmentMap) {
        for (TopicPartition topicPartition : nonAffinitySet) {
            String minMemberId = null;
            int minPartitionNum = Integer.MAX_VALUE;
            for (Map.Entry<String, List<TopicPartition>> entry : resultAssignmentMap.entrySet()) {
                if (entry.getValue().size() <= minPartitionNum) {
                    minPartitionNum = entry.getValue().size();
                    minMemberId = entry.getKey();
                }
            }
            resultAssignmentMap.get(minMemberId).add(topicPartition);
        }
    }

    /**
     * 在MemberInfo列表中寻找对应brokerIP的元素
     *
     * @param consumerList  consumer列表
     * @param brokerIp  broker ip
     * @return  如果找到，则返回其对象，否则，返回 null
     */
    private List<MemberInfo> findMemberInfoByIp(List<MemberInfo> consumerList, String brokerIp) {
        List<MemberInfo> memberInfos = new ArrayList<>();
        for (MemberInfo memberInfo : consumerList) {
            String ip = fetchIpByMemberId(memberInfo.memberId);
            if (ip.equals(brokerIp)) {
                memberInfos.add(memberInfo);
            }
        }
        return memberInfos;
    }

    /**
     * member-id的格式如下：
     * consumer-192.168.1.1-affinity-9ddae128-37b5-4158-8d99-5fea5e1027e0
     *
     * @param memberId 客户端id
     * @return  提取到的ip，例如192.168.1.1
     */
    private String fetchIpByMemberId(String memberId) {
        // 使用-分隔符分割字符串
        String[] parts = memberId.split("-");
        if (parts.length >= 2) {
            return parts[1];
        } else {
            return "";
        }

    }

    private String assembleAllTopicMetadata(Set<String> topics) {
        StringBuilder sb = new StringBuilder();
        for (String topic : topics) {
            List<PartitionInfo> partitionInfoList = metadata.availablePartitionsForTopic(topic);
            for (PartitionInfo partitionInfo : partitionInfoList) {
                if (partitionInfo != null) {
                    Node leader = partitionInfo.leader();
                    String leaderHost = leader == null ? "" : leader.host();
                    sb.append(partitionInfo.topic()).append("-")
                            .append(partitionInfo.partition()).append("-[")
                            .append(leaderHost).append("], ");
                }
            }
        }
        return sb.toString();
    }

    /**
     * 查询对应TopicPartition其所在的broker ip
     *
     * @param topicPartition    分区对象
     * @return  分区所在的broker ip
     */
    private String findPartitionLeaderBrokerIp(TopicPartition topicPartition) {
        List<PartitionInfo> partitionInfoList = metadata.availablePartitionsForTopic(topicPartition.topic());
        for (PartitionInfo partitionInfo : partitionInfoList) {
            if (partitionInfo.partition() == topicPartition.partition()) {
                return partitionInfo.leader().host();
            }
        }

        log.warn("metadata missing, can NOT find the leader broker ip for topicPartition {}", topicPartition);
        return null;
    }

    /**
     * 入口方法，遵循kafka协议
     *
     * @param metadata Current topic/broker metadata known by consumer
     * @param groupSubscription Subscriptions from all members including metadata provided through {@link #subscriptionUserData(Set)}
     * @return  最终的分配方案
     */
    @Override
    public GroupAssignment assign(Cluster metadata, GroupSubscription groupSubscription) {
        long beginTime = System.currentTimeMillis();
        this.metadata = metadata;
        Map<String, Subscription> subscriptions = groupSubscription.groupSubscription();
        Set<String> allSubscribedTopics = new HashSet<>();
        for (Map.Entry<String, Subscription> subscriptionEntry : subscriptions.entrySet())
            allSubscribedTopics.addAll(subscriptionEntry.getValue().topics());

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        for (String topic : allSubscribedTopics) {
            Integer numPartitions = metadata.partitionCountForTopic(topic);
            if (numPartitions != null && numPartitions > 0)
                partitionsPerTopic.put(topic, numPartitions);
            else
                log.debug("Skipping assignment for topic {} since no metadata is available", topic);
        }

        Map<String, List<TopicPartition>> rawAssignments = assign(partitionsPerTopic, subscriptions);

        // this class maintains no user data, so just wrap the results
        Map<String, Assignment> assignments = new HashMap<>();
        for (Map.Entry<String, List<TopicPartition>> assignmentEntry : rawAssignments.entrySet())
            assignments.put(assignmentEntry.getKey(), new Assignment(assignmentEntry.getValue()));
        log.info("affinity rebalance finished, total time cost {}", (System.currentTimeMillis() - beginTime));
        return new GroupAssignment(assignments);
    }
}
