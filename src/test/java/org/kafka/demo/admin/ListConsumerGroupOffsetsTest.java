package org.kafka.demo.admin;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ListConsumerGroupOffsetsTest extends AdminTest {


    /**
     * 此单测对应的执行脚本为 sh kafka-consumer-groups.sh --bootstrap-server localhost:9092 --all-groups --all-topics --describe
     * 如果出现对应的topic-partition缺失leader：
     *  1. 正在进行leader选举
     *  2. topic设置为了单副本，broker宕机后，leader将会缺失
     *
     * 那么调用adminClient.listOffsets接口时将会报错
     *
     * java.util.concurrent.ExecutionException: org.apache.kafka.common.errors.TimeoutException: Call(callName=metadata, deadlineMs=1750748019067, tries=4, nextAllowedTryMs=1750747959475) timed out at 1750747959375 after 4 attempt(s)
     *
     * 	at org.apache.kafka.common.internals.KafkaFutureImpl.wrapAndThrow(KafkaFutureImpl.java:45)
     * 	at org.apache.kafka.common.internals.KafkaFutureImpl.access$000(KafkaFutureImpl.java:32)
     * 	at org.apache.kafka.common.internals.KafkaFutureImpl$SingleWaiter.await(KafkaFutureImpl.java:89)
     * 	at org.apache.kafka.common.internals.KafkaFutureImpl.get(KafkaFutureImpl.java:260)
     * 	at org.kafka.demo.admin.GroupOffsetTest.describeGroupOffsetLag(GroupOffsetTest.java:44)
     * 	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
     * 	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:77)
     * 	at java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
     * 	at java.base/java.lang.reflect.Method.invoke(Method.java:568)
     * 	at org.junit.runners.model.FrameworkMethod$1.runReflectiveCall(FrameworkMethod.java:59)
     * 	at org.junit.internal.runners.model.ReflectiveCallable.run(ReflectiveCallable.java:12)
     * 	at org.junit.runners.model.FrameworkMethod.invokeExplosively(FrameworkMethod.java:56)
     * 	at org.junit.internal.runners.statements.InvokeMethod.evaluate(InvokeMethod.java:17)
     * 	at org.junit.runners.ParentRunner$3.evaluate(ParentRunner.java:306)
     * 	at org.junit.runners.BlockJUnit4ClassRunner$1.evaluate(BlockJUnit4ClassRunner.java:100)
     * 	at org.junit.runners.ParentRunner.runLeaf(ParentRunner.java:366)
     * 	at org.junit.runners.BlockJUnit4ClassRunner.runChild(BlockJUnit4ClassRunner.java:103)
     * 	at org.junit.runners.BlockJUnit4ClassRunner.runChild(BlockJUnit4ClassRunner.java:63)
     * 	at org.junit.runners.ParentRunner$4.run(ParentRunner.java:331)
     * 	at org.junit.runners.ParentRunner$1.schedule(ParentRunner.java:79)
     * 	at org.junit.runners.ParentRunner.runChildren(ParentRunner.java:329)
     * 	at org.junit.runners.ParentRunner.access$100(ParentRunner.java:66)
     * 	at org.junit.runners.ParentRunner$2.evaluate(ParentRunner.java:293)
     * 	at org.junit.runners.ParentRunner$3.evaluate(ParentRunner.java:306)
     * 	at org.junit.runners.ParentRunner.run(ParentRunner.java:413)
     * 	at org.junit.runner.JUnitCore.run(JUnitCore.java:137)
     * 	at com.intellij.junit4.JUnit4IdeaTestRunner.startRunnerWithArgs(JUnit4IdeaTestRunner.java:69)
     * 	at com.intellij.rt.junit.IdeaTestRunner$Repeater$1.execute(IdeaTestRunner.java:38)
     * 	at com.intellij.rt.execution.junit.TestsRepeater.repeat(TestsRepeater.java:11)
     * 	at com.intellij.rt.junit.IdeaTestRunner$Repeater.startRunnerWithArgs(IdeaTestRunner.java:35)
     * 	at com.intellij.rt.junit.JUnitStarter.prepareStreamsAndStart(JUnitStarter.java:232)
     * 	at com.intellij.rt.junit.JUnitStarter.main(JUnitStarter.java:55)
     * Caused by: org.apache.kafka.common.errors.TimeoutException: Call(callName=metadata, deadlineMs=1750748019067, tries=4, nextAllowedTryMs=1750747959475) timed out at 1750747959375 after 4 attempt(s)
     * Caused by: org.apache.kafka.common.errors.LeaderNotAvailableException: There is no leader for this topic-partition as we are in the middle of a leadership election.
     *
     * 使用脚本执行时，可能只会反馈一个超时异常，非常具有迷惑性
     *
     */
    @Test
    public void describeGroupOffsetLag() throws Exception {
        String groupName = "group1";
        try (AdminClient adminClient = createAdminClient()) {
            ListConsumerGroupOffsetsResult result = adminClient.listConsumerGroupOffsets(groupName);
            Map<TopicPartition, OffsetAndMetadata> map = result.partitionsToOffsetAndMetadata().get();

            Map<TopicPartition, OffsetSpec> topicPartitionOffsets = new HashMap<>();
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : map.entrySet()) {
                TopicPartition topicPartition = entry.getKey();
                topicPartitionOffsets.put(topicPartition, OffsetSpec.latest());
            }

            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> leoMap = adminClient.listOffsets(topicPartitionOffsets).all().get();

            List<OffsetBean> offsetBeans = new ArrayList<>();
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : map.entrySet()) {
                TopicPartition topicPartition = entry.getKey();
                long commitedOffset = entry.getValue().offset();
                long leo = leoMap.get(topicPartition).offset();
                offsetBeans.add(new OffsetBean(topicPartition, leo, commitedOffset));
            }

            printResult(offsetBeans);
        }
    }

    private void printResult(List<OffsetBean> offsetBeans) {
        offsetBeans.sort((o1, o2) -> {
            if (o1.topicPartition.topic().equals(o2.topicPartition.topic())) {
                int partition1 = o1.topicPartition.partition();
                int partition2 = o2.topicPartition.partition();
                return Integer.compare(partition1, partition2);
            }
            return 0;
        });
        for (OffsetBean offsetBean : offsetBeans) {
            System.out.println(offsetBean);
        }
    }

    private static class OffsetBean {
        private OffsetBean(TopicPartition topicPartition, long logEndOffset, long commitedOffset) {
            this.topicPartition = topicPartition;
            this.logEndOffset = logEndOffset;
            this.commitedOffset = commitedOffset;
            this.lag = logEndOffset - commitedOffset;
        }
        private TopicPartition topicPartition;
        private long logEndOffset;
        private long commitedOffset;
        private long lag;

        @Override
        public String toString() {
            return String.format("TopicPartition %s, commitedOffset %s, LEO %s, lag %d", topicPartition, commitedOffset, logEndOffset, lag);
        }
    }
}
