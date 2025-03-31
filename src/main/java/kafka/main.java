package kafka;

import java.util.*;

public class main {
    
    public static void main(String[] args) {
        System.out.println(":::: new  22 !!!");
        UtilTools.parseArgs(args);
        Map<String, String> props = UtilTools.properties;
        if (props.get("sync-consume").equals("true")) {
            runConsumer();
        } else if (props.get("sync-send").equals("true")) {
            runProducer();
        } else if (props.get("multi-send").equals("true")) {
            runMultiSendProducer();
        } else if (props.get("single-send").equals("true")) {
            runSingleSendProducer();
        }
    }
    
    public static void runProducer() {
        SyncSendMessage.multiSend(false);
    }
    
    public static void runConsumer() {
        SyncCommitConsumer.consume();
    }

    public static void runMultiSendProducer() {
        SyncSendMessage.multiSend(true);
    }
    
    public static void runSingleSendProducer() {
        SingleSendMessage.sendMsg();
    }
}
