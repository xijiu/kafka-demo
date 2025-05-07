package org.kafka.demo;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

public class DescribeConfigTest extends AdminTest {


    @Test
    public void describeConfigTest() throws Exception {
        try (AdminClient adminClient = createAdminClient()) {
            DescribeConfigsResult result = adminClient.describeConfigs(Collections.singletonList(new ConfigResource(ConfigResource.Type.TOPIC, "topicA")));
            Map<ConfigResource, Config> configMap = result.all().get();
            for (Map.Entry<ConfigResource, Config> entry : configMap.entrySet()) {
                System.out.println("ConfigResource is: " + entry.getKey());
                Config config = entry.getValue();
                Collection<ConfigEntry> entryCollection = config.entries();
                for (ConfigEntry configEntry : entryCollection) {
                    System.out.println("ConfigEntry is: " + configEntry);
                }
            }
            System.out.println("for test ::: " + configMap);
        }
    }

    @Test
    public void describeConfigTestForBroker() throws Exception {
        try (AdminClient adminClient = createAdminClient()) {
            DescribeConfigsResult result = adminClient.describeConfigs(Collections.singletonList(new ConfigResource(ConfigResource.Type.BROKER, "1000")));
            Map<ConfigResource, Config> configMap = result.all().get();
            for (Map.Entry<ConfigResource, Config> entry : configMap.entrySet()) {
                System.out.println("ConfigResource is: " + entry.getKey());
                Config config = entry.getValue();
                Collection<ConfigEntry> entryCollection = config.entries();
                for (ConfigEntry configEntry : entryCollection) {
                    System.out.println("ConfigEntry is: " + configEntry);
                }
            }
            System.out.println("for test ::: " + configMap);
        }
    }
}
