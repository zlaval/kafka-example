package com.zlrx.kafkaexample

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*

fun main() {
    val producer = ProducerDemo()
    producer.sendData("Hello world1")
    producer.sendData("Hello world2")
    producer.sendData("Hello world3")
    producer.sendData("Hello world4")
    producer.flushAndClose()
}

class ProducerDemo {

    private val properties: Properties = Properties()
    private val kafkaProducer: KafkaProducer<String, String>

    init {
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092")
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.qualifiedName)
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.qualifiedName)
        kafkaProducer = KafkaProducer(properties)
    }

    fun sendData(message: String) {
        val record = ProducerRecord<String, String>("first_topic", message)
        kafkaProducer.send(record)
    }

    fun flushAndClose() {
        kafkaProducer.close()
    }

}