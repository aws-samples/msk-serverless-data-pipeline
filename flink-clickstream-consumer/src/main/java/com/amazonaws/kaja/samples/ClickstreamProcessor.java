package com.amazonaws.kaja.samples;

import com.amazonaws.kaja.samples.utils.AmazonOpenSearchSink;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import com.amazonaws.services.schemaregistry.flink.avro.GlueSchemaRegistryAvroDeserializationSchema;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import samples.clickstream.avro.ClickEvent;

import java.time.Duration;
import java.util.*;


public class ClickstreamProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(ClickstreamProcessor.class);
    private static final List<String> MANDATORY_PARAMETERS = Arrays.asList("BootstrapServers");
    private static transient Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss.SSS").create();
    private static Map<String, Object> schemaRegistryConfigs = new HashMap<>();

    public static <T> String toJson(T objToConvert){

        return gson.toJson(objToConvert);
    }

    private static <T> String tupleToString(T tupleToConvert) {
         if (tupleToConvert instanceof Tuple){
             List<String> returnVal = new ArrayList<>();
             Integer arity = ((Tuple) tupleToConvert).getArity();
             for (Integer i = 0; i < arity; i++){
                 returnVal.add(((Tuple) tupleToConvert).getField(i).toString());
             }
             return  String.join(",", returnVal);
         }
        return null;
    }

    public static void main(String[] args) throws Exception {

        //Setting up the ExecutionEnvironment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //Setting TimeCharacteristic
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Map<String, Properties> applicationProperties;

        if (env instanceof LocalStreamEnvironment) {
            //applicationProperties  = KinesisAnalyticsRuntime.getApplicationProperties(Objects.requireNonNull(ClickstreamProcessor.class.getClassLoader().getResource("KDAApplicationProperties.json")).getPath());
            applicationProperties  = KinesisAnalyticsRuntime.getApplicationProperties("/home/ec2-user/flink-clickstream-consumer/src/main/resources/KDAApplicationProperties.json");
            //Setting parallelism in code. When running on my laptop I was getting out of file handles error, so reduced parallelism, but increase it on KDA
            env.setParallelism(1);
            //Setting the Checkpoint interval. The Default for KDA App is 60,000 (or 1 min).
            // Here I'm setting it to 5 secs in the code which will override the KDA app setting
            env.enableCheckpointing(20000L);

        } else {
            applicationProperties  = KinesisAnalyticsRuntime.getApplicationProperties();
        }

        if (applicationProperties == null) {
            LOG.error("Unable to load application properties from the Kinesis Analytics Runtime. Exiting.");

            return;
        }

        Properties flinkProperties = applicationProperties.get("FlinkApplicationProperties");

        if (flinkProperties == null) {
            LOG.error("Unable to load FlinkApplicationProperties properties from the Kinesis Analytics Runtime. Exiting.");

            return;
        }

        if (! flinkProperties.keySet().containsAll(MANDATORY_PARAMETERS)) {
            LOG.error("Missing mandatory parameters. Expected '{}' but found '{}'. Exiting.",
                    String.join(", ", MANDATORY_PARAMETERS),
                    flinkProperties.keySet());

            return;
        }

	LOG.info("Region: " + flinkProperties.getProperty("Region"));

        //Setting properties for Apache kafka (MSK)
        Properties kafkaConfig = new Properties();
        kafkaConfig.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, flinkProperties.getProperty("BootstrapServers"));
        kafkaConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG,flinkProperties.getProperty("GroupId", "flink-clickstream-processor"));
        kafkaConfig.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        kafkaConfig.setProperty(SaslConfigs.SASL_MECHANISM, "AWS_MSK_IAM");
        kafkaConfig.setProperty(SaslConfigs.SASL_JAAS_CONFIG, "software.amazon.msk.auth.iam.IAMLoginModule required;");
        kafkaConfig.setProperty(SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS, "software.amazon.msk.auth.iam.IAMClientCallbackHandler");

        WatermarkStrategy watermarkStrategy = WatermarkStrategy
                .forBoundedOutOfOrderness(Duration.ofSeconds(20)).withIdleness(Duration.ofMinutes(1));

        schemaRegistryConfigs.put(AWSSchemaRegistryConstants.AWS_REGION, flinkProperties.getProperty("Region", Regions.getCurrentRegion().getName()));
        schemaRegistryConfigs.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, AvroRecordType.SPECIFIC_RECORD.getName());

        FlinkKafkaConsumer<ClickEvent> consumer = new FlinkKafkaConsumer<>(
                flinkProperties.getProperty("Topic", "ExampleTopic"),
                GlueSchemaRegistryAvroDeserializationSchema.forSpecific(ClickEvent.class, schemaRegistryConfigs),
                kafkaConfig);
        //Setting the source for Apache kafka (MSK) and assigning Timestamps and watermarks for Event Time
        DataStream<ClickEvent> clickEvents = env.addSource(consumer
                                .setStartFromEarliest()
                                .assignTimestampsAndWatermarks(watermarkStrategy));



        //Using Session windows with a gap of 1 sec since the Clickstream data generated uses a random gap
        // between 50 and 550 msecs
        //Creating User sessions and calculating the total number of events per session, no. of events before a buy
        //and unique departments(products) visited
        DataStream<UserIdSessionEvent> userSessionsAggregates = clickEvents
                .keyBy(event -> event.getUserid())
                .window(EventTimeSessionWindows.withGap(Time.seconds(1)))
                .aggregate(new UserAggregate(), new UserAggWindowFunction());

        //Filtering out the sessions without a buy, so we only have the sessions with a buy
        DataStream<UserIdSessionEvent> userSessionsAggregatesWithOrderCheckout = userSessionsAggregates
                .filter((FilterFunction<UserIdSessionEvent>) userIdSessionEvent -> userIdSessionEvent.getOrderCheckoutEventCount() != 0);

        //Calculating overall number of user sessions in the last 10 seconds (tumbling window), sessions with a buy and
        //percent of sessions with a buy
        //using a processwindowfunction in addition to aggregate to optimize the aggregation
        // and include window metadata like window start and end times
        DataStream<UserIdAggEvent> clickEventsUserIdAggResult =  userSessionsAggregates
                .keyBy(event -> event.getEventKey())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new UserSessionAggregates(), new UserSessionWindowFunction());

        //Calculating unique departments visited and number of user sessions visiting the department
        // in the last 10 seconds (tumbling window)
        //This changes the key to key on departments
        DataStream<DepartmentsAggEvent> departmentsAgg = userSessionsAggregates
                .flatMap(new DepartmentsFlatMap())
                .keyBy(event -> event.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new DepartmentsAggReduceFunction(), new DepartmentsAggWindowFunction());

        //clickEventsUserIdAggResult.addSink(sinkclickEventsUserIdAggResult);
        //Keyed serialization schema to provide a key which with null partitioner will allow Kafka to use the key to
        //hash partition messages across the topic partitions
        FlinkKafkaProducer<DepartmentsAggEvent> kafkaDepts = new FlinkKafkaProducer<>(flinkProperties.getProperty("DepartmentsAgg_Topic", "Departments_Agg"),
                (SerializationSchema<DepartmentsAggEvent>) element -> toJson(element).getBytes(), kafkaConfig, Optional.ofNullable((FlinkKafkaPartitioner<DepartmentsAggEvent>) null));

        kafkaDepts.setWriteTimestampToKafka(true);
        departmentsAgg.addSink(kafkaDepts);

        //Non keyed serialization schema which with null partitioner will allow Kafka to
        //round robin messages across the topic partitions
        FlinkKafkaProducer<UserIdAggEvent> kafkaUserIdAggEvents = new FlinkKafkaProducer<>(flinkProperties.getProperty("clickEventsUserIdAggResult_Topic", "ClickEvents_UserId_Agg_Result"),
                (SerializationSchema<UserIdAggEvent>) element -> toJson(element).getBytes(), kafkaConfig, Optional.ofNullable((FlinkKafkaPartitioner<UserIdAggEvent>) null));

        kafkaUserIdAggEvents.setWriteTimestampToKafka(true);
        clickEventsUserIdAggResult.addSink(kafkaUserIdAggEvents);

        //Non keyed serialization schema which with null partitioner will allow Kafka to
        //round robin messages across the topic partitions
        FlinkKafkaProducer<UserIdSessionEvent> kafkaUserIdSessionEvent = new FlinkKafkaProducer<>(flinkProperties.getProperty("userSessionsAggregatesWithOrderCheckout_Topic", "User_Sessions_Aggregates_With_Order_Checkout"),
                (SerializationSchema<UserIdSessionEvent>) element -> toJson(element).getBytes(), kafkaConfig, Optional.ofNullable((FlinkKafkaPartitioner<UserIdSessionEvent>) null));
        kafkaUserIdSessionEvent.setWriteTimestampToKafka(true);
        userSessionsAggregatesWithOrderCheckout.addSink(kafkaUserIdSessionEvent);

        DataStream<Object> departmentsAggJson = departmentsAgg.map(x -> toJson(x));
        DataStream<Object> clickEventsUserIdAggResultJson = clickEventsUserIdAggResult.map(x -> toJson(x));
        DataStream<Object> userSessionsAggregatesWithOrderCheckoutJson = userSessionsAggregatesWithOrderCheckout.map(x -> toJson(x));

        //Creating Amazon Elasticsearch sinks and sinking the streams to it
        if (flinkProperties.containsKey("OpenSearchEndpoint")) {
            String region;
            final String elasticsearchEndpoint = flinkProperties.getProperty("OpenSearchEndpoint");

            if (env instanceof LocalStreamEnvironment) {
                region = flinkProperties.getProperty("Region");
            } else {
                region = flinkProperties.getProperty("Region", Regions.getCurrentRegion().getName());
            }

            departmentsAggJson.addSink(AmazonOpenSearchSink.buildOpenSearchSink(elasticsearchEndpoint, region, "departments_count"));
            clickEventsUserIdAggResultJson.addSink(AmazonOpenSearchSink.buildOpenSearchSink(elasticsearchEndpoint, region, "user_session_counts"));
            userSessionsAggregatesWithOrderCheckoutJson.addSink(AmazonOpenSearchSink.buildOpenSearchSink(elasticsearchEndpoint, region, "user_session_details"));
        }
        LOG.info("Starting execution..");
        env.execute();

    }
}
