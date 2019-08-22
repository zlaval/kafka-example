package com.zlrx.kafkaexample.twitter

import org.apache.http.HttpHost
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.common.xcontent.XContentType
import java.time.Duration
import java.util.*

fun main() {
    val elasticSearchConsumer = ElasticSearchConsumer()
    elasticSearchConsumer.run()
}

class ElasticSearchConsumer {

    private val client: RestHighLevelClient = createClient()
    private val kafkaConsumer: KafkaConsumer<String, String> = createKafkaConsumer()

    private fun createClient(): RestHighLevelClient {
        val hostName = "kafka-course-9056331690.eu-central-1.bonsaisearch.net"
        val userName = ""
        val password = ""

        val credentialProvider = BasicCredentialsProvider()
        credentialProvider.setCredentials(AuthScope.ANY, UsernamePasswordCredentials(userName, password))
        val restClientBuilder = RestClient.builder(HttpHost(hostName, 443, "https"))
            .setHttpClientConfigCallback { httpAsyncClientBuilder ->
                httpAsyncClientBuilder.setDefaultCredentialsProvider(
                    credentialProvider
                )
            }
        return RestHighLevelClient(restClientBuilder)
    }

    private fun createKafkaConsumer(): KafkaConsumer<String, String> {
        val properties = Properties()
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092")
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.qualifiedName)
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.qualifiedName)
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "elasticsearch_twitter_consumer")
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100")
        val kafkaConsumer: KafkaConsumer<String, String> = KafkaConsumer(properties)
        kafkaConsumer.subscribe(Collections.singleton("twitter"))
        return kafkaConsumer
    }

    fun run() {
        while (true) {
            val records = kafkaConsumer.poll(Duration.ofMillis(100))
            val bulkRequest = BulkRequest()
            records.forEach {
                val id = it.topic() + it.partition() + it.offset()
                val request = IndexRequest("twitter").id(id).source(it.value(), XContentType.JSON)
                bulkRequest.add(request)
            }
            if (!bulkRequest.requests().isEmpty()) {
                val response = client.bulk(bulkRequest, RequestOptions.DEFAULT)
                println(response)
                kafkaConsumer.commitSync()
            }
        }
        //client.close()
    }


}