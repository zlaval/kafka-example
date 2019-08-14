package com.zlrx.kafkaexample

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.util.*

fun main() {
    val producer = ProducerDemo()
    producer.sendData("Hello world1", "a")
    producer.sendData("Hello world2", "b")
    producer.sendData("Hello world3", "c")
    producer.sendData("Hello world4", "c")
    producer.sendData("Hello world5", "c")
    producer.sendData("Hello world6", "a")
    producer.sendData("Hello world7", "b")
    producer.sendData("Hello world8", "b")
    producer.sendData("Hello world9", "c")
    producer.sendData("Hello world10", "d")
    producer.sendData("Hello world11", "e")
    producer.flushAndClose()
}

class ProducerDemo {

    private val logger = LoggerFactory.getLogger(ProducerDemo::class.java)

    private val properties: Properties = Properties()
    private val kafkaProducer: KafkaProducer<String, String>

    init {
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092")
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.qualifiedName)
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.qualifiedName)
        kafkaProducer = KafkaProducer(properties)
    }

    fun sendData(message: String, key: String) {
        val record = ProducerRecord<String, String>("first_topic", key, message)
        logger.info("Key is {}", key)
        kafkaProducer.send(record) { metadata, exception ->
            handleProducedMessageCallback(exception, metadata)
        }
            //block the send to make sync for testing
            .get()
    }

    private fun handleProducedMessageCallback(exception: Exception?, metadata: RecordMetadata) {
        if (exception == null) {
            logger.info(
                "Topic: {}, Partition: {}, Offset: {}, Timestamp: {} ",
                metadata.topic(),
                metadata.partition(),
                metadata.offset(),
                metadata.timestamp()
            )
        } else {
            logger.error(exception.message, exception)
        }
    }

    fun flushAndClose() {
        kafkaProducer.close()
    }

}