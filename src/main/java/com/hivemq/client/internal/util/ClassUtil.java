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

package com.hivemq.client.internal.util;

import org.jetbrains.annotations.NotNull;

/**
 * @author Silvio Giebl
 */
public final class ClassUtil {

    // 这个函数就是对某个类是否存在的方法做了封装，对抛出的异常做了处理
    // 这里的className需要时全路径
    public static boolean isAvailable(final @NotNull String className) {
        try {
            Class.forName(className);
            return true;
        } catch (final ClassNotFoundException e) {
            return false;
        }
    }

    private ClassUtil() {}

    public static void main(String[] args) {
        System.out.println(isAvailable("com.hivemq.client.internal.ClassUtil"));
    }
}
