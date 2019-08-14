package com.zlrx.kafkaexample

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.*

fun main() {
    val consumerDemo = ConsumerDemo()
    consumerDemo.receiveData()
}


class ConsumerDemo {

    private val logger = LoggerFactory.getLogger(ProducerDemo::class.java)

    private val properties: Properties = Properties()
    private val kafkaConsumer: KafkaConsumer<String, String>

    init {
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092")
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.qualifiedName)
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.qualifiedName)
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group_id_example")
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        kafkaConsumer = KafkaConsumer(properties)
    }

    fun receiveData() {
        kafkaConsumer.subscribe(Collections.singleton("first_topic"))
        while (true) {
            val records = kafkaConsumer.poll(Duration.ofMillis(100))
            records.forEach {
                logger.info("New record received ${it.key()} - ${it.value()}")
            }
        }
    }

}