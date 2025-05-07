package kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class UtilTools {
    public static Map<String, String> properties = new HashMap<>();
    
    public static void parseArgs(String[] args) {
//        CommandLine commandLine = null;
//        CommandLineParser parser = new DefaultParser();
//        Options options = new Options();
//        options.addOption(Option.builder().longOpt("sync-consume").hasArg(false).desc("run sync commit consumer").build());
//        options.addOption(Option.builder().longOpt("async-mode").hasArg(false).desc("async mode").build());
//        options.addOption(Option.builder().longOpt("sync-send").hasArg(false).desc("run sync send message producer").build());
//        options.addOption(Option.builder().longOpt("multi-send").hasArg(false).desc("run multi-thread send message producer").build());
//        options.addOption(Option.builder().longOpt("single-send").hasArg(false).desc("run single-thread send message producer").build());
//
//        options.addOption(Option.builder().longOpt("topic").hasArg(true).desc("the topic to send messages").build());
//        options.addOption(Option.builder().longOpt("msg-size").hasArg(true).desc("send message size").build());
//        options.addOption(Option.builder().longOpt("msg-num").hasArg(true).desc("the number of messages to send").build());
//        options.addOption(Option.builder().longOpt("batch-num").hasArg(true).desc("the number of messages in a batch").build());
//        options.addOption(Option.builder().longOpt("thread-num").hasArg(true).desc("the number of thread to send messages").build());
//        options.addOption(Option.builder().longOpt("linger-ms").hasArg(true).desc("wait some time before the producer sends the record batch").build());
//        options.addOption(Option.builder().longOpt("server").hasArg(true).desc("broker to connect").build());
//        options.addOption(Option.builder().longOpt("batch-size").hasArg(true).desc("batch size").build());
//        options.addOption(Option.builder().longOpt("pull-records").hasArg(true).desc("pull records one time").build());
//        options.addOption(Option.builder().longOpt("acks").hasArg(true).desc("producer acks").build());
//
//        try {
//            commandLine = parser.parse(options, args, true);
//        } catch (MissingOptionException exp) {
//            System.out.println(exp.getMessage());
//            System.exit(0);
//        } catch (ParseException e) {
//            throw new RuntimeException(e);
//        }
//
//        properties.put("async-mode", commandLine.hasOption("async-mode") ? "true" : "false");
//        properties.put("sync-consume", commandLine.hasOption("sync-consume") ? "true" : "false");
//        properties.put("sync-send", commandLine.hasOption("sync-send") ? "true" : "false");
//        properties.put("multi-send", commandLine.hasOption("multi-send") ? "true" : "false");
//        properties.put("single-send", commandLine.hasOption("single-send") ? "true" : "false");
//
//        properties.put("topic", commandLine.getOptionValue("topic"));
//        properties.put("msg-size", commandLine.getOptionValue("msg-size"));
//        properties.put("msg-num", commandLine.getOptionValue("msg-num"));
//        properties.put("batch-num", commandLine.getOptionValue("batch-num"));
//        properties.put("thread-num", commandLine.getOptionValue("thread-num"));
//        properties.put("linger-ms", commandLine.getOptionValue("linger-ms"));
//        properties.put("bootstrap-servers", commandLine.getOptionValue("server"));
//        properties.put("batch-size", commandLine.getOptionValue("batch-size"));
//        properties.put("acks", commandLine.getOptionValue("acks"));
        
        String bootstrapServers;
        if (System.getProperty("os.name").toLowerCase().contains("mac")) {
            bootstrapServers = "127.0.0.1:9092";
        } else {
            bootstrapServers = properties.get("bootstrap-servers") == null ? System.getenv("KAFKA_BROKER") : properties.get("bootstrap-servers");
        }
        properties.put("bootstrap-servers", bootstrapServers);
        String useSASL = System.getenv("ACL_ENABLE") == null ? "false" : System.getenv("ACL_ENABLE");
        properties.put("useSASL", useSASL);
        if (useSASL != null && useSASL.equals("true")) {
            properties.put("user", System.getenv("SUPER_USER"));
            properties.put("password", System.getenv("SUPER_USER_PASS"));
        }
        System.out.println("Parameters used: ");
        properties.forEach((k, v) -> System.out.println(k + ":" + v));
    }
    

    
    public static KafkaProducer<byte[], byte[]> createProducer() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, properties.get("bootstrap-servers"));
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "kafka-sync-send-producer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, String.format("%d", 1024*1024*10));
        
        props.put(ProducerConfig.RETRIES_CONFIG, 1);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
//        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10);
//        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 21);
//        setSASL(props);
        
        String lingerMs = properties.get("linger-ms");
        if (lingerMs != null && !lingerMs.equals("0")) {
            props.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs);
        }
        String batchSize = properties.get("batch-size");
        if (batchSize != null && !batchSize.equals("0")) {
            props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
        }

        props.put(ProducerConfig.ACKS_CONFIG, "0");
        return new KafkaProducer<>(props);
    }
    
    public static AdminClient createAdminClient(){
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, properties.get("bootstrap-servers"));
        props.put(AdminClientConfig.RETRIES_CONFIG, 3);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 20000);
        setSASL(props);
        return AdminClient.create(props);
    }
    
    public static long getMsgNum() {
        return properties.get("msg-num") == null ? 0 : Long.parseLong(properties.get("msg-num"));
    }
    
    public static long getMsgSize() {
        return properties.get("msg-size") == null ? 0 : Long.parseLong(properties.get("msg-size"));
    }
    
    public static int getBatchNum() {
        return properties.get("batch-num") == null ? 0 : Integer.parseInt(properties.get("batch-num"));
    }
    
    public static int getThreadNum() {
        return properties.get("thread-num") == null ? 0 : Integer.parseInt(properties.get("thread-num"));
    }
    
    public static byte[] makeMsg(long size) {
        return ByteBuffer.allocate((int) size).array();
    }
    
    
    private static void setSASL(Properties props) {
        String user = "kafka-dpgv7ukghk";
        String password = "dpgv7ukxkm";
        props.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-256");
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        String jaasConfig = "org.apache.kafka.common.security.scram.ScramLoginModule required username='testUser' password='testPassWord';";
        props.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig.replace("testUser", user).replace("testPassWord", password));
    }
}
