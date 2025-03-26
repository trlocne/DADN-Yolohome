package Utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonUtils {
    private static final AvroMapper avroMapper = new AvroMapper();
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Logger logger = LoggerFactory.getLogger(JsonUtils.class);

    public static <T> String toJson(T data) {
        return toJson(data, objectMapper);
    }

    public static <T> String toAvroJson(T data, Class<T> clazz) {
        return toJson(data, avroMapper, new AvroSchema(ReflectData.get().getSchema(clazz)));
    }

    private static <T> String toJson(T data, ObjectMapper mapper) {
        try {
            return mapper.writeValueAsString(data);
        } catch (JsonProcessingException e) {
            logger.error("Serialization error: {}", data, e);
            return "{}";
        }
    }

    private static <T> String toJson(T data, AvroMapper mapper, AvroSchema schema) {
        try {
            return mapper.writer(schema).writeValueAsString(data);
        } catch (JsonProcessingException e) {
            logger.error("Avro serialization error: {}", data, e);
            return "{}";
        }
    }
}
