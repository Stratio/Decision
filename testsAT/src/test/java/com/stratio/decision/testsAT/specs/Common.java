package com.stratio.decision.testsAT.specs;

import kafka.consumer.KafkaStream;
import kafka.utils.ZkUtils;
import net.sf.expectit.Expect;

import org.I0Itec.zkclient.ZkClient;

import com.stratio.specs.CommonG;
import com.stratio.decision.api.IStratioStreamingAPI;
import com.stratio.decision.commons.messages.StratioStreamingMessage;

public class Common extends CommonG {

    private IStratioStreamingAPI stratioStreamingAPI;
    private String KAFKA_HOST;
    private int KAFKA_PORT;
    private String ZOOKEEPER_HOST;
    private int ZOOKEEPER_PORT;
    private String ZOOKEEPER_PATH;
    private Expect shellIface;
    private KafkaStream<String, StratioStreamingMessage> streamListener;

    public IStratioStreamingAPI getStratioStreamingAPI() {
        return stratioStreamingAPI;
    }

    public void setStratioStreamingAPI(IStratioStreamingAPI stratioStreamingAPI) {
        this.stratioStreamingAPI = stratioStreamingAPI;
    }

    public String getKAFKA_HOST() {
        return KAFKA_HOST;
    }

    public void setKAFKA_HOST(String kAFKA_HOST) {
        KAFKA_HOST = kAFKA_HOST;
    }

    public int getKAFKA_PORT() {
        return KAFKA_PORT;
    }

    public void setKAFKA_PORT(int kAFKA_PORT) {
        KAFKA_PORT = kAFKA_PORT;
    }

    public String getZOOKEEPER_HOST() {
        return ZOOKEEPER_HOST;
    }

    public void setZOOKEEPER_HOST(String zOOKEEPER_HOST) {
        ZOOKEEPER_HOST = zOOKEEPER_HOST;
    }

    public String getZOOKEEPER_PATH() {
        return ZOOKEEPER_PATH;
    }

    public void setZOOKEEPER_PATH(String zOOKEEPER_PATH) {
        ZOOKEEPER_PATH = zOOKEEPER_PATH;
    }


    public int getZOOKEEPER_PORT() {
        return ZOOKEEPER_PORT;
    }

    public void setZOOKEEPER_PORT(int zOOKEEPER_PORT) {
        ZOOKEEPER_PORT = zOOKEEPER_PORT;
    }

    public Expect getShellIface() {
        return shellIface;
    }

    public void setShellIface(Expect shellIface) {
        this.shellIface = shellIface;
    }

    public KafkaStream<String, StratioStreamingMessage> getStreamListener() {
        return streamListener;
    }

    public void setStreamListener(
            KafkaStream<String, StratioStreamingMessage> streamListener) {
        this.streamListener = streamListener;
    }

    public int pollZKForTopics(String sign, Integer value) {
        ZkClient zkClient = new ZkClient(this.getZOOKEEPER_HOST() + ":"
                + this.getZOOKEEPER_PORT());

        Boolean stop = false;
        int v = 0;
        int retries = 0;
        try {
            while ((retries < 20) && !stop) {
                v = ZkUtils.getAllTopics(zkClient).size();
                if ("=".equals(sign)) {
                    if (v == value) {
                        stop = true;
                    } else {
                        Thread.sleep(1000);
                    }
                } else if (">=".equals(sign)) {
                    if (v >= value) {
                        stop = true;
                    } else {
                        Thread.sleep(1000);
                    }
                }
                retries++;
            }
        } catch (InterruptedException e) {
            this.getLogger().info("Interrupted polling to Zookeeper");
        } finally {
            zkClient.close();
        }

        return v;
    }
}