package com.hivemq.client.mqtt.examples;

import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class SubscribeExample {

    public static void main(String[] args) throws InterruptedException {
        final Mqtt5BlockingClient client = Mqtt5Client.builder()
                .identifier(UUID.randomUUID().toString())
                .serverHost("localhost")
                .serverPort(1883)
                .buildBlocking();

        client.connect();

        try (final Mqtt5BlockingClient.Mqtt5Publishes publishes = client.publishes(MqttGlobalPublishFilter.ALL)) {

            client.subscribeWith().topicFilter("test/topic").qos(MqttQos.AT_LEAST_ONCE).send();

            publishes.receive(10, TimeUnit.SECONDS).ifPresent(p -> {
                System.out.println(StandardCharsets.UTF_8.decode(p.getPayload().get()));
            });
            publishes.receive(10, TimeUnit.SECONDS).ifPresent(System.out::println);

        } finally {
            client.disconnect();
        }
    }

}
