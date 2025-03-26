package Flink;

import Deserializer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import Utils.JsonUtils;
import com.yolohome.*;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.elasticsearch7.shaded.org.apache.http.HttpHost;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.action.index.IndexRequest;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.client.Requests;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.common.xcontent.XContentType;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
    


public class DataStreamYoloHome {
    private static final Logger LOG = LoggerFactory.getLogger(DataStreamYoloHome.class);
    private static final String jdbcUrl = "jdbc:postgresql://localhost:5432/postgres";
    private static final String username = "postgres";
    private static final String password = "postgres";

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);

        KafkaSource<LedDevice> LedDevice = KafkaSource.<LedDevice>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("led_schema")
                .setGroupId("led-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new LedDeviceDeserializer())
                .build();
        DataStream<LedDevice> LedData = env.fromSource(LedDevice, WatermarkStrategy.noWatermarks(), "LedEvents").returns(LedDevice.class)
                .filter(led -> led.getIdLed() != null);

        KafkaSource<FanDevice> FanStream = KafkaSource.<FanDevice>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("fan_schema")
                .setGroupId("fan-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new FanDeviceDeserializer())
                .build();
        DataStream<FanDevice> FanData = env.fromSource(FanStream, WatermarkStrategy.noWatermarks(), "FanEvents").returns(FanDevice.class)
                .filter(fan -> fan.getIdFan() != null);

        KafkaSource<DoorDevice> DoorStream = KafkaSource.<DoorDevice>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("door_schema")
                .setGroupId("door-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new DoorDeviceDeserializer())
                .build();
        DataStream<DoorDevice> DoorData = env.fromSource(DoorStream, WatermarkStrategy.noWatermarks(), "DoorEvents").returns(DoorDevice.class)
                .filter(door -> door.getIdDoor() != null);

        KafkaSource<HumidityDevice> HumidityStream = KafkaSource.<HumidityDevice>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("hum_schema")
                .setGroupId("humidity-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new HumidityDeviceDeserializer())
                .build();
        DataStream<HumidityDevice> HumidityData = env.fromSource(HumidityStream, WatermarkStrategy.noWatermarks(), "HumidityEvents").returns(HumidityDevice.class)
                .filter(humidity -> humidity.getIdHum() != null)
                .filter(humidity -> humidity.getBbcHum() >= 0 && humidity.getBbcHum() <= 100);

        KafkaSource<TempDevice> TempStream = KafkaSource.<TempDevice>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("tem_schema")
                .setGroupId("temp-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new TempDeviceDeserializer())
                .build();
        DataStream<TempDevice> TempData = env.fromSource(TempStream, WatermarkStrategy.noWatermarks(), "TempEvents").returns(TempDevice.class)
                .filter(temp -> temp.getIdTemp() != null)
                .filter(temp -> temp.getBbcTemp() >= -50 && temp.getBbcTemp() <= 100);

        LedData.print("LED Status: ");
        FanData.print("Fan Status: ");
        DoorData.print("Door Status: ");
        HumidityData.print("Humidity Level: ");
        TempData.print("Temperature: ");

        DataStream<Tuple5<String, Double, String, String, String>> avgTempStream = TempData
                .map(new MapFunctionTemp())
                .keyBy(t0 -> t0.f0)
                .window(SlidingProcessingTimeWindows.of(Time.minutes(2), Time.minutes(1)))
                .aggregate(new AverageAggregate());


        DataStream<Tuple5<String, Double, String, String, String>> avgHempStream = HumidityData
                .map(new MapFunctionHum())
                .keyBy(t0 -> t0.f0)
                .window(SlidingProcessingTimeWindows.of(Time.minutes(2), Time.minutes(1)))
                .aggregate(new AverageAggregate());

        avgTempStream.print("Avg Temperature: ");
        avgHempStream.print("Avg Hemp Level: ");

        JdbcExecutionOptions jobExecutionOptions = new JdbcExecutionOptions.Builder()
                .withBatchSize(1000)
                .withBatchIntervalMs(200)
                .withMaxRetries(5)
                .build();

        JdbcConnectionOptions connectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(jdbcUrl)
                .withDriverName("org.postgresql.Driver")
                .withUsername(username)
                .withPassword(password)
                .build();

        FanData.addSink(JdbcSink.sink(
                "INSERT INTO Fan (device_id, name, status, location, speed) VALUES (?, ?, ?, ?, ?) " +
                        "ON CONFLICT (device_id) " +
                        "DO UPDATE SET " +
                        "   name = COALESCE(EXCLUDED.name, Fan.name), " +
                        "   status = COALESCE(EXCLUDED.status, Fan.status), " +
                        "   location = COALESCE(EXCLUDED.location, Fan.location), " +
                        "   speed = COALESCE(EXCLUDED.speed, Fan.speed)",
                (JdbcStatementBuilder<FanDevice>) (ps, t) -> {
                    ps.setString(1, t.getIdFan() != null ? t.getIdFan().toString() : null);
                    ps.setString(2, null);
                    ps.setBoolean(3, t.getBbcFan() != null && Boolean.parseBoolean(t.getBbcFan().toString()));
                    ps.setString(4, null);
                    ps.setInt(5, t.getBbcFan() != null ? Integer.parseInt(t.getBbcFan().toString()) : 0);
                },
                jobExecutionOptions,
                connectionOptions
        ));

        FanData.addSink(JdbcSink.sink(
                "INSERT INTO Control (device_id, username, control_time) VALUES (?, ?, ?) " +
                        "ON CONFLICT (device_id, username, control_time) DO NOTHING",
                (JdbcStatementBuilder<FanDevice>) (ps, fanDevice) -> {
                    ps.setString(1, fanDevice.getIdFan() != null ? fanDevice.getIdFan().toString() : null);
                    ps.setString(2, fanDevice.getBbcName() != null ? fanDevice.getBbcName().toString() : null);
                    ps.setString(3, fanDevice.getTimestamp() != null ? fanDevice.getTimestamp().toString() : null);
                },
                jobExecutionOptions,
                connectionOptions
        ));

        FanData.sinkTo(
                new Elasticsearch7SinkBuilder<FanDevice>()
                        .setHosts(new HttpHost("localhost", 9200, "http"))
                        .setBulkFlushMaxActions(1000)
                        .setBulkFlushMaxSizeMb(5)
                        .setBulkFlushInterval(1000)
                        .setConnectionTimeout(5000)
                        .setSocketTimeout(60000)
                        .setEmitter((temp, runtimeContext, requestIndexer) -> {
                            try {
                                String json = JsonUtils.toJson(temp);
                                IndexRequest indexRequest = Requests.indexRequest()
                                        .index("FanDevice")
                                        .id(temp.getIdFan().toString())
                                        .source(json, XContentType.JSON);
                                requestIndexer.add(indexRequest);
                            } catch (Exception e) {
                                LOG.error("Error while indexing fan device data: " + e.getMessage(), e);
                            }
                        }).build()
        ).name("Fan Device Sink");

        DoorData.addSink(JdbcSink.sink(
                "INSERT INTO Lock (device_id, name, status, location, password) VALUES (?, ?, ?, ?, ?) " +
                        "ON CONFLICT (device_id) " +
                        "DO UPDATE SET " +
                        "   name = COALESCE(EXCLUDED.name, Lock.name), " +
                        "   status = COALESCE(EXCLUDED.status, Lock.status), " +
                        "   location = COALESCE(EXCLUDED.location, Lock.location), " +
                        "   password = COALESCE(EXCLUDED.password, Lock.password)",
                (JdbcStatementBuilder<DoorDevice>) (ps, t) -> {
                    ps.setString(1, t.getIdDoor() != null ? t.getIdDoor().toString() : null);
                    ps.setString(2, null);
                    ps.setBoolean(3, t.getBbcServo() != null && Boolean.parseBoolean(t.getBbcServo().toString()));
                    ps.setString(4, null);
                    ps.setString(5, t.getBbcPassword() != null ? t.getBbcPassword().toString() : null);
                },
                jobExecutionOptions,
                connectionOptions
        ));

        DoorData.addSink(JdbcSink.sink(
                "INSERT INTO Control (device_id, username, control_time) VALUES (?, ?, ?) " +
                        "ON CONFLICT (device_id, username, control_time) DO NOTHING",
                (JdbcStatementBuilder<DoorDevice>) (ps, doorDevice) -> {
                    ps.setString(1, doorDevice.getIdDoor() != null ? doorDevice.getIdDoor().toString() : null);
                    ps.setString(2, doorDevice.getBbcName() != null ? doorDevice.getBbcName().toString() : null);
                    ps.setString(3, doorDevice.getTimestamp() != null ? doorDevice.getTimestamp().toString() : null);
                },
                jobExecutionOptions,
                connectionOptions
        ));

        DoorData.sinkTo(
                new Elasticsearch7SinkBuilder<DoorDevice>()
                        .setHosts(new HttpHost("localhost", 9200, "http"))
                        .setBulkFlushMaxActions(1000)
                        .setBulkFlushMaxSizeMb(5)
                        .setBulkFlushInterval(1000)
                        .setConnectionTimeout(5000)
                        .setSocketTimeout(60000)
                        .setEmitter((temp, runtimeContext, requestIndexer) -> {
                            try {
                                String json = JsonUtils.toJson(temp);
                                IndexRequest indexRequest = Requests.indexRequest()
                                        .index("DoorDevice")
                                        .id(temp.getIdDoor().toString())
                                        .source(json, XContentType.JSON);
                                requestIndexer.add(indexRequest);
                            } catch (Exception e) {
                                LOG.error("Error while indexing door device data: " + e.getMessage(), e);
                            }
                        }).build()
        ).name("Door Device Sink");

        LedData.addSink(JdbcSink.sink(
                "INSERT INTO Light (device_id, name, status, location) VALUES (?, ?, ?, ?) " +
                        "ON CONFLICT (device_id) " +
                        "DO UPDATE SET " +
                        "   name = COALESCE(EXCLUDED.name, Light.name), " +
                        "   status = COALESCE(EXCLUDED.status, Light.status), " +
                        "   location = COALESCE(EXCLUDED.location, Light.location)",
                (JdbcStatementBuilder<LedDevice>) (ps, t) -> {
                    ps.setString(1, t.getIdLed() != null ? t.getIdLed().toString() : null);
                    ps.setString(2, null);
                    ps.setBoolean(3, t.getBbcLed() != null && Boolean.parseBoolean(t.getBbcLed().toString()));
                    ps.setString(4, null);
                },
                jobExecutionOptions,
                connectionOptions
        ));

        LedData.addSink(JdbcSink.sink(
                "INSERT INTO Control (device_id, username, control_time) VALUES (?, ?, ?) " +
                        "ON CONFLICT (device_id, username, control_time) DO NOTHING",
                (JdbcStatementBuilder<LedDevice>) (ps, ledDevice) -> {
                    ps.setString(1, ledDevice.getIdLed() != null ? ledDevice.getIdLed().toString() : null);
                    ps.setString(2, ledDevice.getBbcName() != null ? ledDevice.getBbcName().toString() : null);
                    ps.setString(3, ledDevice.getTimestamp() != null ? ledDevice.getTimestamp().toString() : null);
                },
                jobExecutionOptions,
                connectionOptions
        ));
        LedData.sinkTo(
                new Elasticsearch7SinkBuilder<LedDevice>()
                        .setHosts(new HttpHost("localhost", 9200, "http"))
                        .setBulkFlushMaxActions(1000)
                        .setBulkFlushMaxSizeMb(5)
                        .setBulkFlushInterval(1000)
                        .setConnectionTimeout(5000)
                        .setSocketTimeout(60000)
                        .setEmitter((temp, runtimeContext, requestIndexer) -> {
                            try {
                                String json = JsonUtils.toJson(temp);
                                IndexRequest indexRequest = Requests.indexRequest()
                                        .index("LedDevice")
                                        .id(temp.getIdLed().toString())
                                        .source(json, XContentType.JSON);
                                requestIndexer.add(indexRequest);
                            } catch (Exception e) {
                                LOG.error("Error while indexing led device data: " + e.getMessage(), e);
                            }
                        }).build()
        ).name("Led Device Sink");

        avgTempStream.addSink(JdbcSink.sink(
                "INSERT INTO Temperature (location, record_time, value) VALUES (?, ?, ?) " +
                        "ON CONFLICT (location, record_time) DO NOTHING",
                (JdbcStatementBuilder<Tuple5<String, Double, String, String, String>>) (ps, t) -> {
                    ps.setString(1, t.f0 != null ? t.f0 : null);
                    ps.setString(2, t.f4 != null ? t.f4 : null);
                    ps.setDouble(3, t.f1);
                },
                jobExecutionOptions,
                connectionOptions
        ));

        avgHempStream.addSink(JdbcSink.sink(
                "INSERT INTO Humidity (location, record_time, value) VALUES (?, ?, ?) " +
                        "ON CONFLICT (location, record_time) DO NOTHING",
                (JdbcStatementBuilder<Tuple5<String, Double, String, String, String>>) (ps, t) -> {
                    ps.setString(1, t.f0 != null ? t.f0 : null);
                    ps.setString(2, t.f4 != null ? t.f4 : null);
                    ps.setDouble(3, t.f1);
                },
                jobExecutionOptions,
                connectionOptions
        ));

        avgTempStream.sinkTo(
                new Elasticsearch7SinkBuilder<Tuple5<String, Double, String, String, String>>()
                        .setHosts(new HttpHost("localhost", 9200, "http"))
                        .setBulkFlushMaxActions(1000)
                        .setBulkFlushMaxSizeMb(5)
                        .setBulkFlushInterval(1000)
                        .setConnectionTimeout(5000)
                        .setSocketTimeout(60000)
                        .setEmitter((temp, runtimeContext, requestIndexer) -> {
                            try {
                                String json = JsonUtils.toJson(temp);
                                IndexRequest indexRequest = Requests.indexRequest()
                                        .index("avgtemp")
                                        .id(temp.f0)
                                        .source(json, XContentType.JSON);
                                requestIndexer.add(indexRequest);
                            } catch (Exception e) {
                                LOG.error("Error while indexing average temperature data: " + e.getMessage(), e);
                            }
                        }).build()
        ).name("Average Temp Sink");

        avgHempStream.sinkTo(
                new Elasticsearch7SinkBuilder<Tuple5<String, Double, String, String, String>>()
                        .setHosts(new HttpHost("localhost", 9200, "http"))
                        .setBulkFlushMaxActions(1000)
                        .setBulkFlushMaxSizeMb(5)
                        .setBulkFlushInterval(1000)
                        .setConnectionTimeout(5000)
                        .setSocketTimeout(60000)
                        .setEmitter((hem, runtimeContext, requestIndexer) -> {
                            try {
                                String json = JsonUtils.toJson(hem);
                                IndexRequest indexRequest = Requests.indexRequest()
                                        .index("avghum")
                                        .id(hem.f0)
                                        .source(json, XContentType.JSON);
                                requestIndexer.add(indexRequest);
                            } catch (Exception e) {
                                LOG.error("Error while indexing average humidity data: " + e.getMessage(), e);
                            }
                        }).build()
        ).name("Average Hum Sink");

        env.execute("YoloHome IoT Data Processing");
    }

}

class MapFunctionTemp implements MapFunction<TempDevice, Tuple5<String, Double, String, String, String>> {
    @Override
    public Tuple5<String, Double, String, String, String> map(TempDevice temp) throws Exception {
        return new Tuple5<String, Double, String, String, String>(temp.getIdTemp().toString(), Double.valueOf(temp.getBbcTemp()), temp.getBbcName().toString(), temp.getBbcPassword().toString(), temp.getTimestamp().toString());
    }

}

class MapFunctionHum implements MapFunction<HumidityDevice, Tuple5<String, Double, String, String, String>> {
    @Override
    public Tuple5<String, Double, String, String, String> map(HumidityDevice hum) throws Exception {
        return new Tuple5<String, Double, String, String, String>(hum.getIdHum().toString(), Double.valueOf(hum.getBbcHum()), hum.getBbcName().toString(), hum.getBbcPassword().toString(), hum.getTimestamp().toString());
    }
}

class AverageAggregate implements AggregateFunction<Tuple5<String, Double, String, String, String>, Tuple6<String, Double, String, String, String, Long>, Tuple5<String, Double, String, String, String>> {
    @Override
    public Tuple6<String, Double, String, String, String, Long> createAccumulator() {
        return new Tuple6<>(null, 0.0, null, null, null, 0L);
    }

    @Override
    public Tuple6<String, Double, String, String, String, Long> add(Tuple5<String, Double, String, String, String> value, Tuple6<String, Double, String, String, String, Long> accumulator) {
        return new Tuple6<>(
                accumulator.f0 != null ? accumulator.f0 : value.f0,
                accumulator.f1 + value.f1,
                accumulator.f2 != null ? accumulator.f2 : value.f2,
                accumulator.f3 != null ? accumulator.f3 : value.f3,
                accumulator.f4 != null ? accumulator.f4 : value.f4,
                accumulator.f5 + 1L
        );
    }

    @Override
    public Tuple5<String, Double, String, String, String> getResult(Tuple6<String, Double, String, String, String, Long> accumulator) {
        return new Tuple5<>(
                accumulator.f0,
                accumulator.f5 > 0 ? accumulator.f1 / accumulator.f5 : 0.0,
                accumulator.f2,
                accumulator.f3,
                accumulator.f4
        );
    }

    @Override
    public Tuple6<String, Double, String, String, String, Long> merge(Tuple6<String, Double, String, String, String, Long> obj1, Tuple6<String, Double, String, String, String, Long> obj2) {
        return new Tuple6<>(
                obj1.f0,
                obj1.f1 + obj2.f1,
                obj1.f2,
                obj1.f3,
                obj1.f4,
                obj1.f5 + obj2.f5
        );
    }
}