package kafka.XW_DDS;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.math.BigDecimal;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Test {

    enum Type {
        A,
        B,
        C,
        D
    }

    public static void main(String[] args) throws InterruptedException {
        BigDecimal bigDecimal = new BigDecimal(Long.MAX_VALUE);
        BigDecimal multiply = bigDecimal.multiply(new BigDecimal(2));
        System.out.println(multiply);
        // 18446744073709551614
        // 18446744073709550615
    }

    private void test1() {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        Runnable task = () -> {
            try {
                String jmxUrl = "service:jmx:rmi:///jndi/rmi://localhost:5555/jmxrmi";
                JMXConnector connector = JMXConnectorFactory.connect(new JMXServiceURL(jmxUrl));
                MBeanServerConnection connection = connector.getMBeanServerConnection();
                String metricName = "java.lang:type=OperatingSystem";
                Optional<ObjectName> first = connection.queryNames(new ObjectName(metricName), null).stream().findFirst();
                StringBuilder sb = new StringBuilder();
                if (first.isPresent()) {
                    Object cpu = connection.getAttribute(first.get(), "ProcessCpuLoad");
                    Object availableProcessors = connection.getAttribute(first.get(), "AvailableProcessors");
                    double cpuVal = (double) cpu * (int) availableProcessors * 100;
                    sb.append("cpu usage ").append(String.format("%.2f", cpuVal)).append("%");
                }

                metricName = "java.lang:type=Memory";
                Optional<ObjectName> second = connection.queryNames(new ObjectName(metricName), null).stream().findFirst();
                if (second.isPresent()) {
                    CompositeData compositeData = (javax.management.openmbean.CompositeData) connection.getAttribute(second.get(), "HeapMemoryUsage");
                    sb.append(", HeapMemoryUsage usage ").append(String.format("%.2f", ((long) compositeData.get("used")) / 1024D / 1024D)).append("MB");
                }
                sb.append(", gc_count="
                                + ManagementFactory.getGarbageCollectorMXBeans().stream()
                                .mapToLong(GarbageCollectorMXBean::getCollectionCount)
                                .sum());
                sb.append(", gc_time="
                                + ManagementFactory.getGarbageCollectorMXBeans().stream()
                                .mapToLong(GarbageCollectorMXBean::getCollectionTime)
                                .sum());
                System.out.println(sb);
            } catch (Exception e) {
                e.printStackTrace();
            }
        };
        scheduler.scheduleAtFixedRate(task, 0, 1, TimeUnit.SECONDS);
    }


}
