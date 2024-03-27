/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.common.attribute;

/**
 * 枚举类型 CQType，表示 Consume Queue（消费队列）的类型。
 * SimpleCQ：表示简单的消费队列类型。在简单消费队列中，每个消息消费进度都被记录为一个单独的条目。
 * BatchCQ：表示批量消费队列类型。在批量消费队列中，多个消息的消费进度可以被组合成一个批次，以减少存储开销并提高读写效率。
 */
public enum CQType {
    SimpleCQ,
    BatchCQ
}
