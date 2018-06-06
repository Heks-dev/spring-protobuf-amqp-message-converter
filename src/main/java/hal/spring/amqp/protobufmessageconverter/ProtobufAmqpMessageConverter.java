package hal.spring.amqp.protobufmessageconverter;

import com.google.protobuf.util.JsonFormat;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.amqp.support.converter.MessageConverter;

import javax.activation.UnsupportedDataTypeException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ProtobufAmqpMessageConverter implements MessageConverter {
    private static final String HEADER = "X-Type";

    private static Map<Class<?>, Method> methodCache = new ConcurrentHashMap<>();
    private static Map<String, Class<?>> typeCache = new ConcurrentHashMap<>();

    @Override
    public Message toMessage(Object object, MessageProperties messageProperties) throws MessageConversionException {
        try {
            supported(object);
            com.google.protobuf.Message mes = (com.google.protobuf.Message) object;
            messageProperties.getHeaders().put(HEADER, mes.getDescriptorForType().getFullName());
            return new Message(JsonFormat.printer().print(mes).getBytes(), messageProperties);
        } catch (Exception e) {
            throw new MessageConversionException(e.getMessage());
        }
    }

    private void supported(Object object) throws UnsupportedDataTypeException {
        if (!(object instanceof com.google.protobuf.Message)) {
            throw new UnsupportedDataTypeException("Type not compatible with protobuf message type");
        }
    }

    @Override
    public Object fromMessage(Message message) throws MessageConversionException {
        try {
            Class<?> clazz = getOrLoad(String.valueOf(message.getMessageProperties().getHeaders().get(HEADER)));
            supported(clazz);
            com.google.protobuf.Message.Builder builder = defineBuilder(clazz);
            String json = new String(message.getBody(), "UTF-8");
            JsonFormat.parser().merge(json, builder);
            return builder.build();
        } catch (Exception e) {
            throw new MessageConversionException(e.getMessage());
        }
    }

    private Class<?> getOrLoad(String type) throws ClassNotFoundException {
        Class<?> clazz = typeCache.get(type);
        if (clazz == null) {
            clazz = Class.forName(type);
            typeCache.put(type, clazz);
        }
        return clazz;
    }

    private void supported(Class<?> clazz) throws UnsupportedDataTypeException {
        if (!com.google.protobuf.Message.class.isAssignableFrom(clazz)) {
            throw new UnsupportedDataTypeException("Type not compatible with protobuf message type");
        }
    }

    private com.google.protobuf.Message.Builder defineBuilder(Class<?> clazz) throws Exception {
        Method method = methodCache.get(clazz);
        if (method == null) {
            method = clazz.getMethod("newBuilder");
            methodCache.put(clazz, method);
        }
        return (com.google.protobuf.Message.Builder) method.invoke(clazz);
    }
}
