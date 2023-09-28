package com.microservices.demo.twitter.to.kakfa.service.runner.impl;

import com.microservices.demo.twitter.to.kakfa.service.config.TwitterToKafkaServiceConfigData;
import com.microservices.demo.twitter.to.kakfa.service.listener.TwitterKafkaStatusListener;
import com.microservices.demo.twitter.to.kakfa.service.runner.StreamRunner;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Component;
import twitter4j.*;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.Arrays;

@Component
public class TwitterKafkaStreamRunner implements StreamRunner {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterKafkaStreamRunner.class);
    private final TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;

    private final TwitterKafkaStatusListener twitterKafkaStatusListener;

    private TwitterStream twitterStream;

    public TwitterKafkaStreamRunner(TwitterToKafkaServiceConfigData configData,
                                    TwitterKafkaStatusListener twitterKafkaStatusListerner) {
        this.twitterToKafkaServiceConfigData = configData;
        this.twitterKafkaStatusListener = twitterKafkaStatusListerner;
    }

    @Override
    public void start() throws TwitterException {
        twitterStream = new TwitterStreamFactory().getInstance();
        twitterStream.addListener(twitterKafkaStatusListener);
        addFilter();
    }

    @PreDestroy
    public void shutdown() {
        if (twitterStream != null) {
            LOG.info("Closing twitter stream!");
            twitterStream.shutdown();
        }
    }

    private void addFilter() {
        String[] keywords = twitterToKafkaServiceConfigData.getTwitterKeywords().toArray(new String[0]);
        FilterQuery filterQuery = new FilterQuery(keywords);
        twitterStream.filter(filterQuery);
        LOG.info("Started filtering twitter stream for keywords {}", Arrays.toString(keywords));
    }
}
