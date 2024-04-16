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

import com.hivemq.client.internal.annotations.NotThreadSafe;
import com.hivemq.client.internal.mqtt.message.publish.MqttStatefulPublish;
import com.hivemq.client.internal.util.collections.HandleList;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import org.jetbrains.annotations.NotNull;

/**
 * @author Silvio Giebl
 * 这个类应该是代表接收到的数据流的各种信息
 * 其中的HandleList代表了这个mqtt消息的订阅方列表
 */
@NotThreadSafe
class MqttStatefulPublishWithFlows extends HandleList<MqttIncomingPublishFlow> {
    // publish中是接收到的mqtt消息
    final @NotNull MqttStatefulPublish publish;
    long id;
    // 和mqtt服务器的连接标识
    long connectionIndex;
    boolean subscriptionFound;
    // 这个字段代表当前消息需要ack的次数，因为一个消息可能有多个订阅方
    private int missingAcknowledgements;

    MqttStatefulPublishWithFlows(final @NotNull MqttStatefulPublish publish) {
        this.publish = publish;
    }

    @Override
    public @NotNull Handle<MqttIncomingPublishFlow> add(final @NotNull MqttIncomingPublishFlow flow) {
        // qos为1或2并且需要手工确认
        if ((publish.stateless().getQos() != MqttQos.AT_MOST_ONCE) && flow.manualAcknowledgement) {
            // 这里是从本条mqtt消息的维度进行没有ack的订阅方的统计
            missingAcknowledgements++;
            // 这里是从订阅方的维度进行没有ack消息的统计
            flow.increaseMissingAcknowledgements();
        }
        // 把订阅方加入到订阅列表里面去
        return super.add(flow);
    }

    boolean areAcknowledged() {
        // 判断还有没有订阅方没有ack
        return missingAcknowledgements == 0;
    }

    // 单个订阅方进行ack，这个订阅方就是 MqttIncomingPublishFlow
    void acknowledge(final @NotNull MqttIncomingPublishFlow flow) {
        flow.acknowledge(--missingAcknowledgements == 0);
    }
}
