package hal.spring.amqp.protobufmessageconverter;

import org.junit.Before;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;

public class ProtobufAmqpMessageConverterTest {

    private ProtobufAmqpMessageConverter messageConverter;

    @Before
    public void setup() {
        messageConverter = new ProtobufAmqpMessageConverter();
    }

    @Test
    public void test() {
        assertTrue(messageConverter != null);
    }
}