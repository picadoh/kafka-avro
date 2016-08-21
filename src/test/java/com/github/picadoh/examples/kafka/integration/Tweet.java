package com.github.picadoh.examples.kafka.integration;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

public class Tweet {

    private final String username;
    private final String tweet;

    public Tweet(String username, String tweet) {
        this.username = username;
        this.tweet = tweet;
    }

    public String getUsername() {
        return username;
    }

    public String getTweet() {
        return tweet;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Tweet tweet1 = (Tweet) o;
        return Objects.equal(username, tweet1.username) &&
                Objects.equal(tweet, tweet1.tweet);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(username, tweet);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("username", username)
                .add("tweet", tweet)
                .toString();
    }
}
