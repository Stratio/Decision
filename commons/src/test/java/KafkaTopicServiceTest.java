import static org.junit.Assert.assertNull;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.decision.commons.kafka.service.KafkaTopicService;

/**
 * Created by eruiz on 5/10/15.
 */
public class KafkaTopicServiceTest {
    private KafkaTopicService func;
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTopicServiceTest.class);


    @Before
    public void setUp() throws Exception {
        LOGGER.debug("Initializing Kafka Topic Service");
        func = new KafkaTopicService("zk.zookeeper.local.dev:2181", "",
                6667, 6000, 6000);
    }

    @Test
    public void testCreateTopicKafka() throws Exception {

        Exception ex = null;
        try {

            func.createOrUpdateTopic("firstTopic", 1, 1);
            func.createTopicIfNotExist("firstTopic", 1, 1);
            func.createTopicIfNotExist("secondTopic", 1, 1);

        } catch (Exception e) {
            ex = e;
            ex.printStackTrace();
        }

        assertNull("Expected null value", ex);
        func.deleteTopics();
        func.close();
    }

    @Test
    public void testCloseKafka() throws Exception {

        Exception ex = null;
        try {

            func.close();

        } catch (Exception e) {
            ex = e;
            ex.printStackTrace();
        }
        assertNull("Expected null value", ex);
    }
}