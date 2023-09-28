package com.microservices.demo.twitter.to.kakfa.service.runner;

import twitter4j.TwitterException;

public interface StreamRunner {
    void start() throws TwitterException;
}
