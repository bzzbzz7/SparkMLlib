package com.dylan.recom.realtime;

/**
 * Created by dylan
 */

import com.alibaba.fastjson.JSON;
import com.dylan.recom.common.Constants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Properties;


public class NewKafkaProducer implements Runnable {
    private static final Logger LOGGER = Logger.getLogger(NewKafkaProducer.class);
    private final String topic;

    public NewKafkaProducer(String topic) {
        this.topic = topic;
    }

    static NewClickEvent[] newClickEvents = new NewClickEvent[]{
            new NewClickEvent(1000000L, 123L),
            new NewClickEvent(1000001L, 111L),
            new NewClickEvent(1000002L, 500L),
            new NewClickEvent(1000003L, 278L),
            new NewClickEvent(1000004L, 681L),
    };

    public void run() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.30.215:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384); //16K
        props.put("linger.ms", 1);  //往kafka服务器提交消息间隔时间，0则立即提交不等待
        props.put("buffer.memory", 33554432);//32M
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<Integer, String> producer = null;
        try {
            System.out.println("Producing messages");
            producer = new KafkaProducer<>(props);
            for (NewClickEvent event : newClickEvents) {
                String eventAsStr = JSON.toJSONString(event);
                ProducerRecord<Integer, String> msg = new ProducerRecord<>(this.topic, eventAsStr);
                producer.send(msg);
                System.out.println("Sending messages:" + eventAsStr);
            }
            System.out.println("Done sending messages");

            //列出topic的相关信息
            List<PartitionInfo> partitions = producer.partitionsFor(topic);
            for (PartitionInfo p : partitions) {
                System.out.println(p);
            }
        } catch (Exception ex) {
            LOGGER.fatal("Error while producing messages", ex);
            LOGGER.trace(null, ex);
            System.err.println("Error while producing messages：" + ex);
        } finally {
            if (producer != null) producer.close();
        }
    }

    public static void main(String[] args) throws Exception {
        new Thread(new NewKafkaProducer(Constants.KAFKA_TOPICS)).start();
    }
}
