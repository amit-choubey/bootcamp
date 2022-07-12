package bootcamp;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.*;


public class Main {


    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        KafkaProducer<String,String> producer = Config.buildProducer();
        ProducerRecord<String,String> record = new ProducerRecord<>(Config.TOPIC, UUID.randomUUID().toString(),"TEST FROM APPLICATION "+UUID.randomUUID().toString());
        Future<RecordMetadata> metadataFuture = producer.send(record,new myCallBack());

        LOG.info("Topic " + metadataFuture.get().topic() + " partition " + metadataFuture.get().partition()
                + " Offset " + metadataFuture.get().offset());


        ExecutorService executorService = Executors.newFixedThreadPool(3);

        Runnable runnable1 = new Runnable() {
            @Override
            public void run() {
                KafkaConsumer<String,String> consumer = Config.buildConsumer();

                consumer.subscribe(Collections.singletonList(Config.TOPIC));

                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    records.forEach(record -> {
                        System.out.println("Topic " + record.topic() + " partition " + record.partition()
                                + " Offset " + record.offset() + "key "+record.key() +" value "+record.value());

                        // store in db , offset in also db
                        // offset  34, committed till 33
                        // * crash *

                        consumer.commitSync();
                    });


                }

            }
        };

        Runnable runnable2 = new Runnable() {
            @Override
            public void run() {
                KafkaConsumer<String,String> consumer = Config.buildConsumer();

                consumer.subscribe(Collections.singletonList(Config.TOPIC));

                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    records.forEach(record -> {
                        System.out.println("Topic " + record.topic() + " partition " + record.partition()
                                + " Offset " + record.offset() + "key "+record.key() +" value "+record.value());

                        // store in db , offset in also db
                        // offset  34, committed till 33
                        // * crash *

                        consumer.commitSync();
                    });


                }

            }
        };


        executorService.submit(runnable1);

        executorService.submit(runnable2);
    }


}
