package com.microservices.demo.twitter.to.kakfa.service;
import com.microservices.demo.twitter.to.kakfa.service.config.TwitterToKafkaServiceConfigData;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SpringBootApplication
public class TwitterToKafkaServiceApplication implements CommandLineRunner {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterToKafkaServiceApplication.class);

    private final TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;

    public TwitterToKafkaServiceApplication(TwitterToKafkaServiceConfigData configData) {
        this.twitterToKafkaServiceConfigData = configData;
    }
    public static void main(String[] args) {SpringApplication.run(TwitterToKafkaServiceApplication.class, args);}

    @Override
    public void run(String... args) throws Exception {
        LOG.info("App starts... ");
        LOG.info(twitterToKafkaServiceConfigData.getTwitterKeywords().toArray(new String[] {}));
        LOG.info(twitterToKafkaServiceConfigData.getWelcomeMessage());
    }
}
