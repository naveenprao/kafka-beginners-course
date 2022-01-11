package com.github.twitter.poc;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static com.github.twitter.poc.TwitterConstants.LOCAL_KAFKA_BROKER;
import static com.github.twitter.poc.TwitterConstants.TWITTER_ACCESS_TOKEN;
import static com.github.twitter.poc.TwitterConstants.TWITTER_ACCESS_TOKEN_SECRET;
import static com.github.twitter.poc.TwitterConstants.TWITTER_API_KEY;
import static com.github.twitter.poc.TwitterConstants.TWITTER_API_KEY_SECRET;

@Slf4j
public class TwitterProducer {
    public static void main(String[] args) throws InterruptedException {
        Configuration config = loadPropertiesFromFile();
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100000);
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        List<String> terms = Lists.newArrayList("bitcoin", "dodecoin", "crypto");
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        hosebirdEndpoint.trackTerms(terms);
        Authentication hosebirdAuth = new OAuth1(config.getString(TWITTER_API_KEY), config.getString(TWITTER_API_KEY_SECRET),
                config.getString(TWITTER_ACCESS_TOKEN), config.getString(TWITTER_ACCESS_TOKEN_SECRET));
        // create twitter client
        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-client-01")
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
        Client hoseBirdClient = builder.build();
        hoseBirdClient.connect();
        // create kafka producer
        KafkaProducer<String, String> kafkaProducer = createKafkaProducer(config.getString(LOCAL_KAFKA_BROKER));
        for (int i = 0; i < 100 && !hoseBirdClient.isDone(); i++) {
            String msg = msgQueue.take();
            ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", msg);
            kafkaProducer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    log.info("***Sent Tweet ***\n" + metadata.toString());
                } else {
                    log.error(exception.getStackTrace().toString());
                }
            });
        }
        kafkaProducer.close();
        hoseBirdClient.stop();
        // loop to send tweets to kafka.
    }

    private static Configuration loadPropertiesFromFile() {
        Parameters params = new Parameters();
        FileBasedConfigurationBuilder<FileBasedConfiguration> builder =
                new FileBasedConfigurationBuilder<FileBasedConfiguration>(PropertiesConfiguration.class)
                        .configure(params.properties()
                                .setFileName("twitter.properties"));
        try {
            Configuration config = builder.getConfiguration();
            return config;
        } catch (ConfigurationException cex) {
            // loading of the configuration file failed
            log.error("Error loading twitter configurations." + cex.getStackTrace());
            System.exit(1);
        }
        return null;
    }

    public static KafkaProducer<String, String> createKafkaProducer(String kafkaBroker) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Create the producers
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        return producer;
    }
}
