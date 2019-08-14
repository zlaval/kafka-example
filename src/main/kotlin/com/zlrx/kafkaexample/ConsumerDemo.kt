package com.zlrx.kafkaexample

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.*
import java.util.concurrent.CountDownLatch

fun main() {
    val latch = CountDownLatch(1)
    val consumer = ConsumerThread(latch)
    val thread = Thread(consumer)
    thread.start()
    Runtime.getRuntime().addShutdownHook(Thread {
        println("Shutdown hook")
        consumer.shutdown()
        latch.await()
    })
    latch.await()
}

class ConsumerThread(val latch: CountDownLatch) : Runnable {

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
        kafkaConsumer.subscribe(Collections.singleton("first_topic"))
    }

    override fun run() {
        try {
            while (true) {
                val records = kafkaConsumer.poll(Duration.ofMillis(100))
                records.forEach {
                    logger.info("New record received ${it.key()} - ${it.value()}")
                }
            }
        } catch (e: WakeupException) {
            logger.error("Received shutdown signal", e)
        } finally {
            kafkaConsumer.close()
            latch.countDown()
        }
    }

    fun shutdown() {
        kafkaConsumer.wakeup()
    }

}