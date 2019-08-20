package com.zlrx.kafkaexample.twitter

import com.google.common.collect.Lists
import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.Constants
import com.twitter.hbc.core.HttpHosts
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.httpclient.BasicClient
import com.twitter.hbc.httpclient.auth.OAuth1
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*
import java.util.concurrent.LinkedBlockingQueue


fun main() {
    val twitterProducer = TwitterProducer()
    twitterProducer.run()
}

class TwitterProducer {

    private fun createTwitterClient(msgQueue: LinkedBlockingQueue<String>): BasicClient {

        val host = HttpHosts(Constants.STREAM_HOST)
        val endpoint = StatusesFilterEndpoint()
        val terms = Lists.newArrayList<String>("kafka", "bitcoin", "java", "usa", "sport", "paintball", "soccer")
        endpoint.trackTerms(terms)
        val auth = OAuth1("", "", "", "")

        return ClientBuilder()
            .name("TwitterApp")
            .hosts(host)
            .authentication(auth)
            .endpoint(endpoint)
            .processor(StringDelimitedProcessor(msgQueue))
            .build()
    }

    fun run() {
        val msgQueue = LinkedBlockingQueue<String>(1_000)
        val client = createTwitterClient(msgQueue)
        client.connect()

        val kafkaProducer = createKafkaProducer()
        Runtime.getRuntime().addShutdownHook(Thread {
            println("stopping application")
            client.stop()
            kafkaProducer.close()
        })

        while (!client.isDone) {

            val msg = msgQueue.poll()
            if (msg != null) {
                val record = ProducerRecord<String, String>("tweets", null, msg)
                kafkaProducer.send(record) { metadata, exception ->
                    if (exception != null) {
                        println(exception)
                    } else {
                        println(msg)
                    }
                }
            }
        }
        println("Application finished")
    }

    private fun createKafkaProducer(): KafkaProducer<String, String> {
        val properties = Properties()
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092")
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.qualifiedName)
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.qualifiedName)

        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all")
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE.toString())
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5")

        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy")
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024))
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20")

        return KafkaProducer(properties)
    }


}