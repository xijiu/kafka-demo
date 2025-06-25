package org.kafka.demo.admin;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SaslConfigs;

import java.util.Properties;

public abstract class AdminTest {
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final boolean USE_SASL = false;
    private static final String USER_NAME = "kafka-egwiwwcls9";
    private static final String PASSWORD = "__CIPHER__V0uCjSXxAa1QMVNDn1fjyT46tfIq/OGDDlQ=";



    protected static AdminClient createAdminClient() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(AdminClientConfig.RETRIES_CONFIG, 3);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 20000);
        setSASL(props);
        return AdminClient.create(props);
    }

    protected static void setSASL(Properties props) {
        if (USE_SASL) {
            props.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-256");
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            String jaasConfig = "org.apache.kafka.common.security.scram.ScramLoginModule required username='testUser' password='testPassWord';";
            props.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig.replace("testUser", USER_NAME).replace("testPassWord", PASSWORD));
        }
    }
}
