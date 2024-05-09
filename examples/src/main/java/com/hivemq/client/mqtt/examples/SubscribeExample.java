package com.hivemq.client.mqtt.examples;

import com.hivemq.client.internal.mqtt.message.publish.MqttPublish;
import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class SubscribeExample {

    public static void main(String[] args) throws InterruptedException {
//        syncConnect();
        asyncConnect();
    }

    private static void syncConnect() throws InterruptedException {
        final Mqtt5BlockingClient client = Mqtt5Client.builder()
                .identifier(UUID.randomUUID().toString())
                .serverHost("192.168.86.128")
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

    private static void asyncConnect() throws InterruptedException {
        Mqtt5AsyncClient client = Mqtt5Client.builder()
                .identifier(UUID.randomUUID().toString())
                .serverHost("192.168.86.128")
                .serverPort(1883)
                .buildAsync();

        client.connect();
//        client.connect()
//                .thenCompose(connAck -> client.publishWith().topic("test/topic").payload("1".getBytes()).send())
//                .thenCompose(publishResult -> client.disconnect());

        client.subscribeWith()
                .topicFilter("test/topic")
                .qos(MqttQos.EXACTLY_ONCE)
                .callback(SubscribeExample::doSomething)
                .send();
        TimeUnit.HOURS.sleep(1);
    }

    private static void doSomething(Mqtt5Publish publish) {
        System.out.println("+++++++++++++++++++++");
        publish.acknowledge();
//        throw new RuntimeException("ops");
    }

}
