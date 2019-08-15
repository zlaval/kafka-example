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
        val terms = Lists.newArrayList<String>("kafka")
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
        return KafkaProducer(properties)
    }


}