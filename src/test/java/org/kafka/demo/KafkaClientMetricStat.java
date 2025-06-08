package org.kafka.demo;

import lombok.extern.slf4j.Slf4j;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 用来统计Producer及Consumer的各项JMX指标详情，旨在用来定位相关问题
 */
@Slf4j
public class KafkaClientMetricStat {

    private static volatile boolean started = false;

    private static volatile ScheduledExecutorService executor = null;

    private static final ProducerMetricsStat producerMetricsStat = new ProducerMetricsStat();

    private static final ConsumerMetricsStat consumerMetricsStat = new ConsumerMetricsStat();

    private static final BasicMetricsStat basicMetricsStat = new BasicMetricsStat();

    private static final String logPrefix = "kafka client metrics collect, ";


    public static void start() {
        start(TimeUnit.SECONDS, 5);
    }

    public static void start(TimeUnit timeUnit, int time) {
        if (!started) {
            synchronized (ProducerMetricsStat.class) {
                // double check to guarantee atomic
                if (!started) {
                    executor = Executors.newSingleThreadScheduledExecutor();
                    Runnable task = KafkaClientMetricStat::doStatMetrics;
                    executor.scheduleAtFixedRate(task, 0, time, timeUnit);
                    started = true;
                }
            }
        }
    }

    private static void doStatMetrics() {
        try {
            long begin = System.currentTimeMillis();
            producerMetricsStat.stat();
            consumerMetricsStat.stat();
            basicMetricsStat.stat();
            log.info("{}total time cost {} ms", logPrefix, (System.currentTimeMillis() - begin));
        } catch (Exception e) {
            log.error("{}collect error", logPrefix, e);
        }
    }

    private static class ProducerMetricsStat {

        private final String[] collectAttrs = {
                "record-queue-time-max", "record-queue-time-avg", "request-latency-avg", "request-latency-max",
                "request-rate", "requests-in-flight", "waiting-threads", "select-rate", "records-per-request-avg",
                "record-send-rate", "outgoing-byte-rate", "network-io-rate"
        };

        private void stat() throws Exception {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            ObjectName namePattern = new ObjectName("kafka.producer:type=producer-metrics,client-id=*");
            Set<ObjectName> mbeanNames = mbs.queryNames(namePattern, null);
            for (ObjectName mbeanName : mbeanNames) {
                AttributeList attributeList = mbs.getAttributes(mbeanName, collectAttrs);
                log.info("{}mbeanName {}, attributes {}", logPrefix, mbeanName, transString(attributeList));
            }
        }
    }

    private static class ConsumerMetricsStat {

        private final String[] collectAttrs = {
                "bytes-consumed-rate", "fetch-latency-avg", "fetch-latency-max", "fetch-rate", "fetch-size-avg",
                "fetch-size-max", "fetch-throttle-time-avg", "fetch-throttle-time-max", "records-consumed-rate",
                "records-lag-max", "records-lead-min", "records-per-request-avg"
        };

        private void stat() throws Exception {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            ObjectName namePattern = new ObjectName("kafka.consumer:type=consumer-fetch-manager-metrics,client-id=*");
            Set<ObjectName> mbeanNames = mbs.queryNames(namePattern, null);
            for (ObjectName mbeanName : mbeanNames) {
                AttributeList attributeList = mbs.getAttributes(mbeanName, collectAttrs);
                log.info("{}mbeanName {}, attributes {}", logPrefix, mbeanName, transString(attributeList));
            }
        }
    }

    private static class BasicMetricsStat {

        private final String[] cpuCollectAttrs = {"ProcessCpuLoad", "AvailableProcessors"};

        private void stat() throws Exception {
            statCpu();
            statMemory();
        }

        private void statMemory() throws Exception {
            ObjectName objectName = new ObjectName("java.lang:type=Memory");

            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            CompositeData compositeData = (javax.management.openmbean.CompositeData) mbs.getAttribute(objectName, "HeapMemoryUsage");
            Object used = compositeData.get("used");
            Object committed = compositeData.get("committed");
            String memoryRate = String.format("%.5f", ((Long) used).doubleValue() / ((Long) committed).doubleValue());
            log.info("{}used size {}, max size {}, memoryRate {} %", logPrefix, used, committed, memoryRate);
        }

        private void statCpu() throws Exception {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            ObjectName mbeanName = new ObjectName("java.lang:type=OperatingSystem");
            AttributeList attributeList = mbs.getAttributes(mbeanName, cpuCollectAttrs);
            List<Attribute> attributes = attributeList.asList();
            Object cpu = attributes.get(0).getValue();
            Object availableProcessors = attributes.get(1).getValue();
            String cpuRate = formatDouble((double) cpu * (int) availableProcessors * 100);
            log.info("{}cpu {}, availableProcessors {}, cpuRate {} %", logPrefix, cpu, availableProcessors, cpuRate);
        }
    }

    private static String transString(AttributeList attributeList) {
        List<Attribute> attributes = attributeList.asList();
        StringBuilder sb = new StringBuilder();
        for (Attribute attribute : attributes) {
            sb.append(attribute.getName()).append("=").append(attribute.getValue()).append(", ");
        }
        return sb.toString();
    }

    private static String formatDouble(double val) {
        return String.format("%.2f", val);
    }


    public static void shutdown() {
        if (executor != null) {
            synchronized (ProducerMetricsStat.class) {
                if (executor != null) {
                    if (!executor.isShutdown()) {
                        executor.shutdownNow();
                        started = false;
                    }
                }
            }
        }
    }
}
