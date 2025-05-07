package kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;

public class SyncSendMessage {

    private static String TOPIC;
    private static long MSG_SIZE = 1 * 1024;
    private static long MSG_TOTAL_NUM = 1000_0000;
    private static int BATCH_NUM = 100;
    private static int THREAD_NUM = 8;
    private static byte[] SINGLE_MSG = null;
    
//    private static LongAdder totalCostTime = new LongAdder();
    private static double totalCostTime;
    private static AtomicIntegerArray costTimeDistributionArray;
    private static final long TIME_THRESHOLD = 1000;
    private static final long STEP_SIZE = 50;
    private static final long MAX_SLOT;

    static {
        int arraySize = (int) (TIME_THRESHOLD / STEP_SIZE + 1);
        costTimeDistributionArray = new AtomicIntegerArray(arraySize);
        MAX_SLOT = arraySize - 1;
    }

    private static void initConfig() {
        MSG_SIZE = UtilTools.getMsgSize() != 0 ? (int) UtilTools.getMsgSize() : MSG_SIZE;
        MSG_TOTAL_NUM = UtilTools.getMsgNum() != 0 ? (int) UtilTools.getMsgNum() : MSG_TOTAL_NUM;
        BATCH_NUM = UtilTools.getBatchNum() != 0 ? UtilTools.getBatchNum() : BATCH_NUM;
        THREAD_NUM = UtilTools.getThreadNum() != 0 ? UtilTools.getThreadNum() : THREAD_NUM;
        SINGLE_MSG = UtilTools.makeMsg(MSG_SIZE);
    }

    private static void printFinalResult(boolean runAsync) {
        System.out.println("msg total size is " + (MSG_SIZE * MSG_TOTAL_NUM / 1024 / 1024) + " MB, and cost time " + (totalCostTime / 1000) + "s, "
            + "throughput is " + computeThroughput());
        if (!runAsync) {
            printTimeDistribution();
        } else {
            System.out.println("avg cost time is " + (totalCostTime / MSG_TOTAL_NUM) + " ms");
        }
    }
    
    private static String computeThroughput() {
        double totalSize = MSG_TOTAL_NUM * MSG_SIZE;
        double throughput = totalSize / (totalCostTime / 1000);
        throughput /= 1024 * 1024;
        return String.format("%.2f MB/s", throughput);
    }
    
    public static void fillTimeDistributionArray(long timeCost) {
        int slot = (int) (timeCost / STEP_SIZE);
        if (slot > MAX_SLOT) slot = (int) MAX_SLOT;
        costTimeDistributionArray.addAndGet(slot, 1);
    }
    
    public static void printTimeDistribution() {
        System.out.println("distribution: " + costTimeDistributionArray);
        for (int i = 0; i < costTimeDistributionArray.length() - 1; i++) {
            System.out.println((i * STEP_SIZE) + "ms" + " ~ " + ((i + 1) * STEP_SIZE - 1) + "ms: " + costTimeDistributionArray.get(i)
                + ", proportion occupied: " + String.format("%.2f", costTimeDistributionArray.get(i) * 1.0 / (MSG_TOTAL_NUM / BATCH_NUM) * 100) + "%");
        }
        System.out.println("more than " + TIME_THRESHOLD + " ms: " + costTimeDistributionArray.get((int) MAX_SLOT));
    }

    private static void doMultiSend(boolean runAsync) {
        AtomicLong totalNum = new AtomicLong(MSG_TOTAL_NUM);
        ExecutorService executorService = Executors.newCachedThreadPool();
        KafkaProducer<byte[], byte[]> producer = UtilTools.createProducer();
        AdminClient kafkaAdmin = UtilTools.createAdminClient();
        
        CountDownLatch latch = new CountDownLatch(THREAD_NUM);
        for (int i = 0; i < THREAD_NUM; i++) {
            executorService.submit(() -> {
                try {
                    while (true) {
                        long result = totalNum.addAndGet(-BATCH_NUM);
                        if (result < 0) {
                            break;
                        }
                        if (result % 100_0000 == 0) {
                            System.out.println("for test ::: messages left: " + result + " total message number: " + MSG_TOTAL_NUM);
                        }
                        doSend(runAsync, producer, kafkaAdmin);
                    }
                } finally {
                    latch.countDown();
                }
            });
        }
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(1200, TimeUnit.SECONDS)) {
                System.err.println("Timeout reached. Forcing shutdown.");
                System.err.println("Interrupted during shutdown. Forcing shutdown.");
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
        }
        
        producer.close();
        kafkaAdmin.close();
    }

    public static void doSend(boolean runAsync, KafkaProducer<byte[], byte[]> producer, AdminClient adminClient) {
        try {
            if (runAsync) {
                asyncSend(producer, adminClient);
            } else {
                SyncSend(producer, adminClient);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private static void asyncSend(KafkaProducer<byte[], byte[]> producer, AdminClient kafkaAdmin) throws Exception {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(TOPIC, SINGLE_MSG);
        for (int i = 0; i < BATCH_NUM; i++) {
            producer.send(record);
//            producer.send(record, (metadata, exception) -> {
//                if (exception != null) {
//                    System.out.println("exception occurs, and offset = " + metadata.offset());
//                    System.out.println("Exception when sending message: " + exception);
//                }
//            });
        }
    }

    public static void SyncSend(KafkaProducer<byte[], byte[]> producer, AdminClient kafkaAdmin) throws Exception {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(TOPIC, SINGLE_MSG);
        long begin = System.currentTimeMillis();
        
        Semaphore semaphore = new Semaphore(0);
        Map<Integer, AtomicLong> offsetMap = new ConcurrentHashMap<>();
        for (int i = 0; i < BATCH_NUM; i++) {
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.out.println("exception when sending message: " + exception);
                    System.exit(1);
                    return;
                }
                int partition = metadata.partition();
                long offset = metadata.offset();
                AtomicLong maxOffset = offsetMap.computeIfAbsent(partition, k -> new AtomicLong(0L));
                while (true) {
                    long current = maxOffset.get();
                    if (offset > current) {
                        if (maxOffset.compareAndSet(current, offset)) {
                            break;
                        }
                    } else {
                        break;
                    }
                }
                semaphore.release();
            });
        }
        
//        while (semaphore.availablePermits() < BATCH_NUM) {
//            System.out.println("for test ::: semaphore number NOT ENOUGH!!!  before sleep: " + semaphore.availablePermits());
//            Thread.sleep(1);
//        }
        semaphore.acquire(BATCH_NUM);


        while (true) {
            try {
                Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap = kafkaAdmin.listConsumerGroupOffsets("").partitionsToOffsetAndMetadata().get();
                boolean allMsgConsumed = true;
                for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : topicPartitionOffsetAndMetadataMap.entrySet()) {
                    TopicPartition topicPartition = entry.getKey();
                    OffsetAndMetadata offsetAndMetadata = entry.getValue();
                    AtomicLong produceOffset = offsetMap.get(topicPartition.partition());
                    if (produceOffset == null) {
                        continue;
                    }
                    if (offsetAndMetadata.offset() < produceOffset.get()) {
                        allMsgConsumed = false;
                        break;
                    }
                }
                if (allMsgConsumed) {
                    fillTimeDistributionArray(System.currentTimeMillis() - begin);
                    break;
                } else {
                    Thread.sleep(1);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
