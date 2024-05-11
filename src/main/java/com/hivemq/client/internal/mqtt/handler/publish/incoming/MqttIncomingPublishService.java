/*
 * Copyright 2018-present HiveMQ and the HiveMQ Community
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hivemq.client.internal.mqtt.handler.publish.incoming;

import com.hivemq.client.internal.annotations.CallByThread;
import com.hivemq.client.internal.checkpoint.Confirmable;
import com.hivemq.client.internal.logging.InternalLogger;
import com.hivemq.client.internal.logging.InternalLoggerFactory;
import com.hivemq.client.internal.mqtt.ioc.ClientScope;
import com.hivemq.client.internal.mqtt.message.publish.MqttPublish;
import com.hivemq.client.internal.util.collections.ChunkedArrayQueue;
import com.hivemq.client.internal.util.collections.HandleList.Handle;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import org.jetbrains.annotations.NotNull;

/**
 * @author Silvio Giebl
 */
// 这个类是独立的，是为所有消息订阅方服务的
@ClientScope
class MqttIncomingPublishService {

    private static final @NotNull InternalLogger LOGGER =
            InternalLoggerFactory.getLogger(MqttIncomingPublishService.class);
    private static final boolean QOS_0_DROP_OLDEST = true; // TODO configurable

    // MqttIncomingQosHandler相当于是上游数据来源
    private final @NotNull MqttIncomingQosHandler incomingQosHandler;
    // MqttIncomingPublishFlows相当于是下游数据处理
    final @NotNull MqttIncomingPublishFlows incomingPublishFlows;

    private final @NotNull ChunkedArrayQueue<MqttStatefulPublishWithFlows> qos0Queue = new ChunkedArrayQueue<>(32);
    private final ChunkedArrayQueue<MqttStatefulPublishWithFlows>.@NotNull Iterator qos0It = qos0Queue.iterator();
    private final @NotNull ChunkedArrayQueue<MqttStatefulPublishWithFlows> qos1Or2Queue = new ChunkedArrayQueue<>(32);
    private final ChunkedArrayQueue<MqttStatefulPublishWithFlows>.@NotNull Iterator qos1Or2It = qos1Or2Queue.iterator();

    private long nextQoS1Or2PublishId = 1;

    private int referencedFlowCount;
    private int runIndex;
    private int blockingFlowCount;

    MqttIncomingPublishService(
            final @NotNull MqttIncomingQosHandler incomingQosHandler,
            final @NotNull MqttIncomingPublishFlows incomingPublishFlows) {

        this.incomingQosHandler = incomingQosHandler;
        this.incomingPublishFlows = incomingPublishFlows;
    }

    @CallByThread("Netty EventLoop")
    void onPublishQos0(final @NotNull MqttStatefulPublishWithFlows publishWithFlows, final int receiveMaximum) {
        // qos0的消息，如果积压消息数量超过限制，直接丢弃
        if (qos0Queue.size() >= receiveMaximum) { // TODO receiveMaximum
            LOGGER.warn("QoS 0 publish message dropped.");
            if (QOS_0_DROP_OLDEST) {
                qos0It.reset();
                final MqttStatefulPublishWithFlows flows = qos0It.next();
                qos0It.remove();
                for (Handle<MqttIncomingPublishFlow> h = flows.getFirst(); h != null; h = h.getNext()) {
                    if (h.getElement().dereference() == 0) {
                        referencedFlowCount--;
                    }
                }
            } else {
                return;
            }
        }
        onPublish(publishWithFlows);
        // 不为空意味着还有订阅者没有处理改消息，需要放进队列里做后续跟踪
        if (!publishWithFlows.isEmpty()) {
            qos0Queue.offer(publishWithFlows);
        }
    }

    @CallByThread("Netty EventLoop")
    boolean onPublishQos1Or2(final @NotNull MqttStatefulPublishWithFlows publishWithFlows, final int receiveMaximum) {
        if (qos1Or2Queue.size() >= receiveMaximum) {
            return false; // flow control error
        }
        publishWithFlows.id = nextQoS1Or2PublishId++;
        onPublish(publishWithFlows);
        if (qos1Or2Queue.isEmpty() && publishWithFlows.isEmpty() && publishWithFlows.areAcknowledged()) {
            incomingQosHandler.ack(publishWithFlows);
        } else {
            qos1Or2Queue.offer(publishWithFlows);
        }
        return true;
    }

    @CallByThread("Netty EventLoop")
    private void onPublish(final @NotNull MqttStatefulPublishWithFlows publishWithFlows) {
        incomingPublishFlows.findMatching(publishWithFlows);
        // 在做了findMatching操作后，publishWithFlows会将匹配的接收方保存在HandleList中
        if (publishWithFlows.isEmpty()) {
            LOGGER.warn("No publish flow registered for {}.", publishWithFlows.publish);
        }
        // 这里我是这么理解的，消息的处理是有先来后到的，要先把队列里能处理的都处理了，才能处理当前的（就是下面的emit操作）
        drain();
        for (Handle<MqttIncomingPublishFlow> h = publishWithFlows.getFirst(); h != null; h = h.getNext()) {
            // 之所以判断 == 1 时才做++操作，是为了保证每个处于工作中（正在处理订阅消息）的Flow只计算一次
            // 这个操作只会在收到消息的时候做，已经放到队列里的就不会有这个操作了
            if (h.getElement().reference() == 1) {
                referencedFlowCount++;
            }
        }
        emit(publishWithFlows);
    }

    @CallByThread("Netty EventLoop")
    void drain() {
        runIndex++;
        blockingFlowCount = 0;

        // 每次都从头开始处理
        qos1Or2It.reset();
        while (qos1Or2It.hasNext()) {
            final MqttStatefulPublishWithFlows publishWithFlows = qos1Or2It.next();
            emit(publishWithFlows);
            // 这里的判断条件有三个
            // 第一个条件用来确保当前的 publishWithFlows 是消息数组中的第一个
            // 第二个条件也是用来保证所有的订阅方都进行了消息的推送
            // 第三个是判断这个消息对应的所有订阅方是否都已经进行了ack
            if ((qos1Or2It.getIterated() == 1) && publishWithFlows.isEmpty() && publishWithFlows.areAcknowledged()) {
                qos1Or2It.remove();
                incomingQosHandler.ack(publishWithFlows);
            } else if (blockingFlowCount == referencedFlowCount) {
                return;
            }
        }

        // 每次都从头开始处理
        qos0It.reset();
        while (qos0It.hasNext()) {
            final MqttStatefulPublishWithFlows publishWithFlows = qos0It.next();
            emit(publishWithFlows);
            if ((qos0It.getIterated() == 1) && publishWithFlows.isEmpty()) {
                qos0It.remove();
            } else if (blockingFlowCount == referencedFlowCount) {
                return;
            }
        }
    }

    @CallByThread("Netty EventLoop")
    private void emit(final @NotNull MqttStatefulPublishWithFlows publishWithFlows) {
        // 轮询订阅方进行当前消息的推送
        for (Handle<MqttIncomingPublishFlow> h = publishWithFlows.getFirst(); h != null; h = h.getNext()) {
            final MqttIncomingPublishFlow flow = h.getElement();

            // 订阅被取消了
            if (flow.isCancelled()) {
                publishWithFlows.remove(h);
                // 之所以判断 == 0 时才做--操作，是为了保证每个处于工作中（正在处理订阅消息）的Flow只计算一次
                if (flow.dereference() == 0) {
                    referencedFlowCount--;
                }
            } else {
                // 这里是一个背压操作，就是限流
                final long requested = flow.requested(runIndex);
                if (requested > 0) {
                    MqttPublish publish = publishWithFlows.publish.stateless();
                    if (flow.manualAcknowledgement) {
                        final Confirmable confirmable;
                        if (publish.getQos() == MqttQos.AT_MOST_ONCE) {
                            confirmable = new MqttIncomingPublishConfirmable.Qos0();
                        } else {
                            confirmable = new MqttIncomingPublishConfirmable(flow, publishWithFlows);
                        }
                        publish = publish.withConfirmable(confirmable);
                    }
                    flow.onNext(publish);
                    publishWithFlows.remove(h);
                    if (flow.dereference() == 0) {
                        referencedFlowCount--;
                        flow.checkDone();
                    }
                } else if (requested == 0) {
                    // requested为0代表向这个订阅方推送消息被阻塞了，阻塞统计+1
                    blockingFlowCount++;
                    // 应该可以这么理解，blockingFlowCount代表当前被阻塞的flow，referencedFlowCount代表当前消费消息的flow
                    // 如果两者相等，意味着所有的处于工作中Flow都被阻塞了，都阻塞了，说明当前业务很繁忙，还发送啥新消息呀，只能break退出循环
                    if (blockingFlowCount == referencedFlowCount) {
                        break;
                    }
                }
            }
        }
    }
}
