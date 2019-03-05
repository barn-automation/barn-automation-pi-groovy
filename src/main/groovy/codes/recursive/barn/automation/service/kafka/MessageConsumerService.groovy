package codes.recursive.barn.automation.service.kafka


import groovy.json.JsonException
import groovy.json.JsonSlurper
import groovy.util.logging.Log4j
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.WakeupException

import java.util.concurrent.atomic.AtomicBoolean

@Log4j
class MessageConsumerService implements KafkaService {

    codes.recursive.barn.automation.service.arduino.ArduinoService arduinoService
    String topicName
    String bootstrapServer
    KafkaConsumer consumer
    String group = "barn_pi_group"
    private final AtomicBoolean closed = new AtomicBoolean(false)

    MessageConsumerService(topic, server, arduinoService) {
        this.topicName = topic
        this.bootstrapServer = server
        this.arduinoService = arduinoService

        Properties props = new Properties()
        props.put("bootstrap.servers", bootstrapServer)
        props.put("group.id", group)
        props.put("enable.auto.commit", "true")
        props.put("auto.commit.interval.ms", "1000")
        props.put("session.timeout.ms", "30000")
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

        consumer = new KafkaConsumer(props)
    }

    def start() {
        try {
            consumer.subscribe(Arrays.asList(topicName))
            log.info("Subscribed to topic ${consumer.subscription()} ")

            while (!closed.get()) {
                ConsumerRecords records = consumer.poll(1000)
                if( records.size() ) {
                    log.info("Received ${records.size()} records")
                }
                records.each { ConsumerRecord record ->
                    log.info("offset: ${record.offset()}, key: ${record.key().toString()}, value: ${record.value()}, timestamp: ${new Date(record.timestamp())}")
                    def msg
                    try {
                        msg = new JsonSlurper().parseText(record.value())
                        arduinoService.send( new codes.recursive.barn.automation.model.ArduinoMessage(msg?.type, msg?.message) )
                    }
                    catch (JsonException e) {
                        log.error("Error parsing JSON from ${record.value()}")
                        e.printStackTrace()
                    }
                    catch (Exception e) {
                        log.error("Error:")
                        e.printStackTrace()
                    }
                    println msg
                }
            }
        }
        catch (WakeupException e) {
            if ( !closed.get() ) {
                log.error("Kafka WakeupException")
                e.printStackTrace()
                throw e
            }
        }
        finally {
            log.info("Closing consumer for ${topicName}")
            consumer.close()
        }
    }

    def close() {
        closed.set(true)
        consumer.wakeup()
    }
}
