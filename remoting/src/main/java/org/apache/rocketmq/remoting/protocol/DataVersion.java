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
package org.apache.rocketmq.remoting.protocol;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Dataversion和topic的配置被加载到内存之后，分别会解析成为topicConfigManager的topicConfigTablehe属性和dataVersion属性
 */
public class DataVersion extends RemotingSerializable {
    private long stateVersion = 0L;

    /**
     * 时间戳毫秒值
     */
    private long timestamp = System.currentTimeMillis();
    /**
     * 版本号
     */
    private AtomicLong counter = new AtomicLong(0);

    /**
     * 拷贝目标dataVersion的数据，在从文件恢复数据的时候会用到
     */
    public void assignNewOne(final DataVersion dataVersion) {
        this.timestamp = dataVersion.timestamp;
        this.stateVersion = dataVersion.stateVersion;
        this.counter.set(dataVersion.counter.get());
    }

    /**
     * nextVersion方法被调用时，将会引起timestamp和counter的改变，一般来说，
     * 新创建broker，或者更新topic的信息的时候nextVersion方法会被调用
     */
    public void nextVersion() {
        this.nextVersion(0L);
    }
    /**
     * 更新时间戳以及counter到下一个版本
     */
    public void nextVersion(long stateVersion) {
        this.timestamp = System.currentTimeMillis();
        this.stateVersion = stateVersion;
        this.counter.incrementAndGet();
    }

    public long getStateVersion() {
        return stateVersion;
    }

    public void setStateVersion(long stateVersion) {
        this.stateVersion = stateVersion;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public AtomicLong getCounter() {
        return counter;
    }

    public void setCounter(AtomicLong counter) {
        this.counter = counter;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        DataVersion version = (DataVersion) o;

        if (getStateVersion() != version.getStateVersion())
            return false;
        if (getTimestamp() != version.getTimestamp())
            return false;

        if (counter != null && version.counter != null) {
            return counter.longValue() == version.counter.longValue();
        }

        return null == counter && null == version.counter;

    }

    @Override
    public int hashCode() {
        int result = (int) (getStateVersion() ^ (getStateVersion() >>> 32));
        result = 31 * result + (int) (getTimestamp() ^ (getTimestamp() >>> 32));
        if (null != counter) {
            long l = counter.get();
            result = 31 * result + (int) (l ^ (l >>> 32));
        }
        return result;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("DataVersion[");
        sb.append("timestamp=").append(timestamp);
        sb.append(", counter=").append(counter);
        sb.append(']');
        return sb.toString();
    }

    public int compare(DataVersion dataVersion) {
        if (this.getStateVersion() > dataVersion.getStateVersion()) {
            return 1;
        } else if (this.getStateVersion() < dataVersion.getStateVersion()) {
            return -1;
        } else if (this.getCounter().get() > dataVersion.getCounter().get()) {
            return 1;
        } else if (this.getCounter().get() < dataVersion.getCounter().get()) {
            return -1;
        } else if (this.getTimestamp() > dataVersion.getTimestamp()) {
            return 1;
        } else if (this.getTimestamp() < dataVersion.getTimestamp()) {
            return -1;
        }
        return 0;
    }
}
