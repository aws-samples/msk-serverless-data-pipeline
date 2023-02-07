package com.amazonaws.kafka.samples;

import com.amazonaws.services.lambda.runtime.events.KafkaEvent;
import com.amazonaws.services.schemaregistry.deserializers.avro.AWSKafkaAvroDeserializer;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import samples.clickstream.avro.ClickEvent;

import java.util.*;

class ProcessRecords {

    private static final Logger logger = LogManager.getLogger(ProcessRecords.class);

    private byte[] base64Decode(KafkaEvent.KafkaEventRecord kafkaEventRecord) {
        return Base64.getDecoder().decode(kafkaEventRecord.getValue().getBytes());

    }

    private long getKafkaEventRecordsSize(KafkaEvent kafkaEvent) {
        long size = 0L;
        for (Map.Entry<String, List<KafkaEvent.KafkaEventRecord>> kafkaEventEntry : kafkaEvent.getRecords().entrySet()) {
            size += kafkaEventEntry.getValue().size();
        }
        return size;
    }

    private Map<String, Object> getGSRConfigs() {

        Map<String, Object> gsrConfigs = new HashMap<>();

        gsrConfigs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        gsrConfigs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AWSKafkaAvroDeserializer.class.getName());
        gsrConfigs.put(AWSSchemaRegistryConstants.AWS_REGION, System.getenv("AWS_REGION"));
        gsrConfigs.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, AvroRecordType.SPECIFIC_RECORD.getName());

        return gsrConfigs;
    }

    private void deserializeAddToS3Batch(Deserializer deserializer, KafkaEvent kafkaEvent, String requestId, S3Uploader s3Uploader) {

        StringBuilder csvData = new StringBuilder();

        final char CR = (char) 0x0D;
        final char LF = (char) 0x0A;
        final String CRLF = "" + CR + LF;
        final String delimiter = ",";

        csvData.append("p,eventtimestamp,devicetype,event_type,product_type,userid,gobalseq,prevglobalseq");
        csvData.append(CRLF);


        kafkaEvent.getRecords().forEach((key, value) -> value.forEach(v -> {

                            ClickEvent clickEvent = null;

                            clickEvent = (ClickEvent) deserializer.deserialize(v.getTopic(), base64Decode(v));

                            if (clickEvent != null) {

                                csvData
                                        .append(clickEvent.getIp().toString())
                                        .append(delimiter)
                                        .append(clickEvent.getEventtimestamp())
                                        .append(delimiter)
                                        .append(clickEvent.getDevicetype().toString())
                                        .append(delimiter)
                                        .append(clickEvent.getEventType().toString())
                                        .append(delimiter)
                                        .append(clickEvent.getProductType().toString())
                                        .append(delimiter)
                                        .append(clickEvent.getUserid())
                                        .append(delimiter)
                                        .append(clickEvent.getGlobalseq())
                                        .append(delimiter)
                                        .append(clickEvent.getPrevglobalseq())
                                        .append(CRLF);
                            }
                        }
                )
        );
        s3Uploader.uploadS3File(csvData.toString());
        logger.info("Uploaded {} records so S3 \n", getKafkaEventRecordsSize(kafkaEvent));

    }

    void processRecords(KafkaEvent kafkaEvent, String requestId) {


        logger.info("Processing batch with {} records for Request ID {} \n", getKafkaEventRecordsSize(kafkaEvent), requestId);

        S3Uploader s3Uploader = new S3Uploader();

        Deserializer deserializer = new AWSKafkaAvroDeserializer(getGSRConfigs());

        deserializeAddToS3Batch(deserializer, kafkaEvent, requestId, s3Uploader);


    }
}
