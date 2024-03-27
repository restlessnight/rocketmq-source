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
package org.apache.rocketmq.store.queue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.attribute.CQType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.common.utils.QueueTypeUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.ConsumeQueue;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.MessageStoreConfig;

import java.io.File;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.lang.String.format;
import static org.apache.rocketmq.store.config.StorePathConfigHelper.getStorePathBatchConsumeQueue;
import static org.apache.rocketmq.store.config.StorePathConfigHelper.getStorePathConsumeQueue;

public class ConsumeQueueStore {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    protected final DefaultMessageStore messageStore;
    protected final MessageStoreConfig messageStoreConfig;
    protected final QueueOffsetAssigner queueOffsetAssigner = new QueueOffsetAssigner();
    protected final ConcurrentMap<String/* topic */, ConcurrentMap<Integer/* queueId */, ConsumeQueueInterface>> consumeQueueTable;

    // Should be careful, do not change the topic config
    // TopicConfigManager is more suitable here.
    private ConcurrentMap<String, TopicConfig> topicConfigTable;

    public ConsumeQueueStore(DefaultMessageStore messageStore, MessageStoreConfig messageStoreConfig) {
        this.messageStore = messageStore;
        this.messageStoreConfig = messageStoreConfig;
        this.consumeQueueTable = new ConcurrentHashMap<>(32);
    }

    public void setTopicConfigTable(ConcurrentMap<String, TopicConfig> topicConfigTable) {
        this.topicConfigTable = topicConfigTable;
    }

    private FileQueueLifeCycle getLifeCycle(String topic, int queueId) {
        //此处为什么强转没有报错 new ConsumeQueue()
        //方法中返回的是ConsumeQueue实例  ConsumeQueue implements ConsumeQueueInterface, FileQueueLifeCycle 所以不报错
        return (FileQueueLifeCycle) findOrCreateConsumeQueue(topic, queueId);
    }

    public long rollNextFile(ConsumeQueueInterface consumeQueue, final long offset) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        return fileQueueLifeCycle.rollNextFile(offset);
    }

    public void correctMinOffset(ConsumeQueueInterface consumeQueue, long minCommitLogOffset) {
        consumeQueue.correctMinOffset(minCommitLogOffset);
    }

    /**
     * Apply the dispatched request and build the consume queue. This function should be idempotent.
     *
     * @param consumeQueue consume queue
     * @param request dispatch request
     */
    public void putMessagePositionInfoWrapper(ConsumeQueueInterface consumeQueue, DispatchRequest request) {
        consumeQueue.putMessagePositionInfoWrapper(request);
    }

    public void putMessagePositionInfoWrapper(DispatchRequest dispatchRequest) {
        ConsumeQueueInterface cq = this.findOrCreateConsumeQueue(dispatchRequest.getTopic(), dispatchRequest.getQueueId());
        this.putMessagePositionInfoWrapper(cq, dispatchRequest);
    }

    /**
     * ConsumeQueue对象建立之后，会对自己管理的队列id目录下面的ConsumeQueue文件进行加载。
     * 内部就是调用mappedFileQueue的load方法，
     * 会对每个ConsumeQueue文件床创建一个MappedFile对象并且进行内存映射mmap操作。
     * @param consumeQueue
     * @return
     */
    public boolean load(ConsumeQueueInterface consumeQueue) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        return fileQueueLifeCycle.load();
    }

    /**
     * 加载Consume Queue文件
     * @return
     */
    public boolean load() {
        boolean cqLoadResult = loadConsumeQueues(getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()), CQType.SimpleCQ);
        boolean bcqLoadResult = loadConsumeQueues(getStorePathBatchConsumeQueue(this.messageStoreConfig.getStorePathRootDir()), CQType.BatchCQ);
        return cqLoadResult && bcqLoadResult;
    }

    /**
     * 该方法用于加载消费队列文件，ConsumeQueue文件可以看作是CommitLog的索引文件，其存储了它所属Topic的消息在Commit Log中的偏移量。
     * 消费者拉取消息的时候，可以从Consume Queue中快速的根据偏移量定位消息在Commit Log中的位置。
     * 一个队列id目录对应着一个ConsumeQueue对象，其内部保存着一个mappedFileQueue对象，其表示当前队列id目录下面的ConsumeQueue文件集合，
     * 同样一个ConsumeQueue文件被映射为一个MappedFile对象。
     * 随后ConsumeQueue及其topic和queueId的对应关系被存入DefaultMessageStore的consumeQueueTable属性集合中。
     * @param storePath
     * @param cqType {@link CQType}
     * @return
     */
    private boolean loadConsumeQueues(String storePath, CQType cqType) {
        //获取ConsumeQueue文件所在目录，目录路径为{storePathRootDir}/consumequeue
        //${RocketMQ_Home}/store/consumequeue/${Topic}/${QueueId}/consumequeue
        File dirLogic = new File(storePath);
        //获取目录下文件列表，实际上下面页是topic目录列表
        File[] fileTopicList = dirLogic.listFiles();
        if (fileTopicList != null) {
            //遍历topic目录
            for (File fileTopic : fileTopicList) {
                //获取topic名字
                String topic = fileTopic.getName();
                //获取topic目录下面的队列id目录
                File[] fileQueueIdList = fileTopic.listFiles();
                if (fileQueueIdList != null) {
                    for (File fileQueueId : fileQueueIdList) {
                        int queueId;
                        try {
                            //获取队列id
                            queueId = Integer.parseInt(fileQueueId.getName());
                        } catch (NumberFormatException e) {
                            continue;
                        }
                        //用于检查指定主题（topic）的消费队列（Consumer Queue）类型是否符合预期。该方法通常被用于在消息发送或消费时进行预检查，以确保消费队列类型的一致性
                        queueTypeShouldBe(topic, cqType);
                        //创建ConsumeQueue对象，一个队列id目录对应着一个ConsumeQueue对象
                        //其内部保存着
                        ConsumeQueueInterface logic = createConsumeQueueByType(cqType, topic, queueId, storePath);
                        //将当然ConsumeQueue对象及其对应关系存入consumeQueueTable中
                        this.putConsumeQueue(topic, queueId, logic);
                        //加载ConsumeQueue文件
                        if (!this.load(logic)) {
                            return false;
                        }
                    }
                }
            }
        }

        log.info("load {} all over, OK", cqType);

        return true;
    }

    private ConsumeQueueInterface createConsumeQueueByType(CQType cqType, String topic, int queueId, String storePath) {
        if (Objects.equals(CQType.SimpleCQ, cqType)) {
            return new ConsumeQueue(
                topic,
                queueId,
                storePath,
                //大小默认30w数据
                this.messageStoreConfig.getMappedFileSizeConsumeQueue(),
                this.messageStore);
        } else if (Objects.equals(CQType.BatchCQ, cqType)) {
            return new BatchConsumeQueue(
                topic,
                queueId,
                storePath,
                //大小默认30w数据
                this.messageStoreConfig.getMapperFileSizeBatchConsumeQueue(),
                this.messageStore);
        } else {
            throw new RuntimeException(format("queue type %s is not supported.", cqType.toString()));
        }
    }

    /**
     * 用于检查指定主题（topic）的消费队列（Consumer Queue）类型是否符合预期。该方法通常被用于在消息发送或消费时进行预检查，以确保消费队列类型的一致性
     * @param topic
     * @param cqTypeExpected
     */
    private void queueTypeShouldBe(String topic, CQType cqTypeExpected) {
        TopicConfig topicConfig = this.topicConfigTable == null ? null : this.topicConfigTable.get(topic);
        // 获取指定主题的消费队列类型
        CQType cqTypeActual = QueueTypeUtils.getCQType(Optional.ofNullable(topicConfig));
        // 如果消费队列类型不符合预期，则抛出异常
        if (!Objects.equals(cqTypeExpected, cqTypeActual)) {
            throw new RuntimeException(format("The queue type of topic: %s should be %s, but is %s", topic, cqTypeExpected, cqTypeActual));
        }
    }

    private ExecutorService buildExecutorService(BlockingQueue<Runnable> blockingQueue, String threadNamePrefix) {
        return new ThreadPoolExecutor(
            this.messageStore.getBrokerConfig().getRecoverThreadPoolNums(),
            this.messageStore.getBrokerConfig().getRecoverThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            blockingQueue,
            new ThreadFactoryImpl(threadNamePrefix));
    }

    /**
     * 同步恢复文件
     * @param consumeQueue
     */
    public void recover(ConsumeQueueInterface consumeQueue) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        fileQueueLifeCycle.recover();
    }

    /**
     * 同步恢复文件
     */
    public void recover() {
        for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueueInterface logic : maps.values()) {
                this.recover(logic);
            }
        }
    }

    /**
     * 异步恢复文件
     * @return
     */
    public boolean recoverConcurrently() {
        int count = 0;
        for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : this.consumeQueueTable.values()) {
            count += maps.values().size();
        }
        final CountDownLatch countDownLatch = new CountDownLatch(count);
        BlockingQueue<Runnable> recoverQueue = new LinkedBlockingQueue<>();
        final ExecutorService executor = buildExecutorService(recoverQueue, "RecoverConsumeQueueThread_");
        List<FutureTask<Boolean>> result = new ArrayList<>(count);
        try {
            for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : this.consumeQueueTable.values()) {
                for (final ConsumeQueueInterface logic : maps.values()) {
                    FutureTask<Boolean> futureTask = new FutureTask<>(() -> {
                        boolean ret = true;
                        try {
                            ((FileQueueLifeCycle) logic).recover();
                        } catch (Throwable e) {
                            ret = false;
                            log.error("Exception occurs while recover consume queue concurrently, " +
                                "topic={}, queueId={}", logic.getTopic(), logic.getQueueId(), e);
                        } finally {
                            countDownLatch.countDown();
                        }
                        return ret;
                    });

                    result.add(futureTask);
                    executor.submit(futureTask);
                }
            }
            countDownLatch.await();
            for (FutureTask<Boolean> task : result) {
                if (task != null && task.isDone()) {
                    if (!task.get()) {
                        return false;
                    }
                }
            }
        } catch (Exception e) {
            log.error("Exception occurs while recover consume queue concurrently", e);
            return false;
        } finally {
            executor.shutdown();
        }
        return true;
    }

    public long getMaxOffsetInConsumeQueue() {
        long maxPhysicOffset = -1L;
        for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueueInterface logic : maps.values()) {
                if (logic.getMaxPhysicOffset() > maxPhysicOffset) {
                    maxPhysicOffset = logic.getMaxPhysicOffset();
                }
            }
        }
        return maxPhysicOffset;
    }

    public void checkSelf(ConsumeQueueInterface consumeQueue) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        fileQueueLifeCycle.checkSelf();
    }

    public void checkSelf() {
        for (Map.Entry<String, ConcurrentMap<Integer, ConsumeQueueInterface>> topicEntry : this.consumeQueueTable.entrySet()) {
            for (Map.Entry<Integer, ConsumeQueueInterface> cqEntry : topicEntry.getValue().entrySet()) {
                this.checkSelf(cqEntry.getValue());
            }
        }
    }

    public boolean flush(ConsumeQueueInterface consumeQueue, int flushLeastPages) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        return fileQueueLifeCycle.flush(flushLeastPages);
    }

    public void destroy(ConsumeQueueInterface consumeQueue) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        fileQueueLifeCycle.destroy();
    }

    public int deleteExpiredFile(ConsumeQueueInterface consumeQueue, long minCommitLogPos) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        return fileQueueLifeCycle.deleteExpiredFile(minCommitLogPos);
    }

    public void truncateDirtyLogicFiles(ConsumeQueueInterface consumeQueue, long phyOffset) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        fileQueueLifeCycle.truncateDirtyLogicFiles(phyOffset);
    }

    public void swapMap(ConsumeQueueInterface consumeQueue, int reserveNum, long forceSwapIntervalMs,
        long normalSwapIntervalMs) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        fileQueueLifeCycle.swapMap(reserveNum, forceSwapIntervalMs, normalSwapIntervalMs);
    }

    public void cleanSwappedMap(ConsumeQueueInterface consumeQueue, long forceCleanSwapIntervalMs) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        fileQueueLifeCycle.cleanSwappedMap(forceCleanSwapIntervalMs);
    }

    public boolean isFirstFileAvailable(ConsumeQueueInterface consumeQueue) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        return fileQueueLifeCycle.isFirstFileAvailable();
    }

    public boolean isFirstFileExist(ConsumeQueueInterface consumeQueue) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        return fileQueueLifeCycle.isFirstFileExist();
    }

    public ConsumeQueueInterface findOrCreateConsumeQueue(String topic, int queueId) {
        return doFindOrCreateConsumeQueue(topic, queueId);
    }

    /**
     * 尝试查找指定主题和队列编号的消费队列。
     * 如果找到了对应的消费队列，则返回该消费队列的实例。
     * 如果没有找到对应的消费队列，则创建一个新的消费队列，并返回该消费队列的实例。
     * @param topic
     * @param queueId
     * @return
     */
    private ConsumeQueueInterface doFindOrCreateConsumeQueue(String topic, int queueId) {
        ConcurrentMap<Integer, ConsumeQueueInterface> map = consumeQueueTable.get(topic);
        if (null == map) {
            ConcurrentMap<Integer, ConsumeQueueInterface> newMap = new ConcurrentHashMap<>(128);
            ConcurrentMap<Integer, ConsumeQueueInterface> oldMap = consumeQueueTable.putIfAbsent(topic, newMap);
            if (oldMap != null) {
                map = oldMap;
            } else {
                map = newMap;
            }
        }

        ConsumeQueueInterface logic = map.get(queueId);
        if (logic != null) {
            return logic;
        }

        ConsumeQueueInterface newLogic;

        Optional<TopicConfig> topicConfig = this.getTopicConfig(topic);
        // TODO maybe the topic has been deleted.
        if (Objects.equals(CQType.BatchCQ, QueueTypeUtils.getCQType(topicConfig))) {
            newLogic = new BatchConsumeQueue(
                topic,
                queueId,
                getStorePathBatchConsumeQueue(this.messageStoreConfig.getStorePathRootDir()),
                this.messageStoreConfig.getMapperFileSizeBatchConsumeQueue(),
                this.messageStore);
        } else {
            newLogic = new ConsumeQueue(
                topic,
                queueId,
                getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()),
                this.messageStoreConfig.getMappedFileSizeConsumeQueue(),
                this.messageStore);
        }

        ConsumeQueueInterface oldLogic = map.putIfAbsent(queueId, newLogic);
        if (oldLogic != null) {
            logic = oldLogic;
        } else {
            logic = newLogic;
        }

        return logic;
    }

    public Long getMaxOffset(String topic, int queueId) {
        return this.queueOffsetAssigner.currentQueueOffset(topic + "-" + queueId);
    }

    public void setTopicQueueTable(ConcurrentMap<String, Long> topicQueueTable) {
        this.queueOffsetAssigner.setTopicQueueTable(topicQueueTable);
        this.queueOffsetAssigner.setLmqTopicQueueTable(topicQueueTable);
    }

    public ConcurrentMap getTopicQueueTable() {
        return this.queueOffsetAssigner.getTopicQueueTable();
    }

    public void setBatchTopicQueueTable(ConcurrentMap<String, Long> batchTopicQueueTable) {
        this.queueOffsetAssigner.setBatchTopicQueueTable(batchTopicQueueTable);
    }

    public void assignQueueOffset(MessageExtBrokerInner msg, short messageNum) {
        ConsumeQueueInterface consumeQueue = findOrCreateConsumeQueue(msg.getTopic(), msg.getQueueId());
        consumeQueue.assignQueueOffset(this.queueOffsetAssigner, msg, messageNum);
    }

    public void updateQueueOffset(String topic, int queueId, long offset) {
        String topicQueueKey = topic + "-" + queueId;
        this.queueOffsetAssigner.updateQueueOffset(topicQueueKey, offset);
    }

    public void removeTopicQueueTable(String topic, Integer queueId) {
        this.queueOffsetAssigner.remove(topic, queueId);
    }

    public ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueueInterface>> getConsumeQueueTable() {
        return consumeQueueTable;
    }

    private void putConsumeQueue(final String topic, final int queueId, final ConsumeQueueInterface consumeQueue) {
        ConcurrentMap<Integer/* queueId */, ConsumeQueueInterface> map = this.consumeQueueTable.get(topic);
        if (null == map) {
            map = new ConcurrentHashMap<>();
            map.put(queueId, consumeQueue);
            this.consumeQueueTable.put(topic, map);
        } else {
            map.put(queueId, consumeQueue);
        }
    }

    /**
     * 在对consumequeue和commitlog进行恢复之后，之后会对consumeQueueTable进行恢复。
     * topicQueueTable存储的是“topic-queueid”到当前queueId下面最大的相对偏移量的map
     * @param minPhyOffset
     */
    public void recoverOffsetTable(long minPhyOffset) {
        ConcurrentMap<String, Long> cqOffsetTable = new ConcurrentHashMap<>(1024);
        ConcurrentMap<String, Long> bcqOffsetTable = new ConcurrentHashMap<>(1024);
        //遍历consumeQueueTable，即consumequeue文件的集合
        for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueueInterface logic : maps.values()) {
                String key = logic.getTopic() + "-" + logic.getQueueId();

                long maxOffsetInQueue = logic.getMaxOffsetInQueue();
                if (Objects.equals(CQType.BatchCQ, logic.getCQType())) {
                    bcqOffsetTable.put(key, maxOffsetInQueue);
                } else {
                    cqOffsetTable.put(key, maxOffsetInQueue);
                }

                this.correctMinOffset(logic, minPhyOffset);
            }
        }

        //Correct unSubmit consumeOffset
        //是否开启重复消息校验
        if (messageStoreConfig.isDuplicationEnable()) {
            SelectMappedBufferResult lastBuffer = null;
            long startReadOffset = messageStore.getCommitLog().getConfirmOffset() == -1 ? 0 : messageStore.getCommitLog().getConfirmOffset();
            while ((lastBuffer = messageStore.selectOneMessageByOffset(startReadOffset)) != null) {
                try {
                    if (lastBuffer.getStartOffset() > startReadOffset) {
                        startReadOffset = lastBuffer.getStartOffset();
                        continue;
                    }

                    ByteBuffer bb = lastBuffer.getByteBuffer();
                    int magicCode = bb.getInt(bb.position() + 4);
                    if (magicCode == CommitLog.BLANK_MAGIC_CODE) {
                        startReadOffset += bb.getInt(bb.position());
                        continue;
                    } else if (magicCode != MessageDecoder.MESSAGE_MAGIC_CODE) {
                        throw new RuntimeException("Unknown magicCode: " + magicCode);
                    }

                    lastBuffer.getByteBuffer().mark();
                    DispatchRequest dispatchRequest = messageStore.getCommitLog().checkMessageAndReturnSize(lastBuffer.getByteBuffer(), true, true, true);
                    if (!dispatchRequest.isSuccess())
                        break;
                    lastBuffer.getByteBuffer().reset();

                    MessageExt msg = MessageDecoder.decode(lastBuffer.getByteBuffer(), true, false, false, false, true);
                    if (msg == null)
                        break;

                    String key = msg.getTopic() + "-" + msg.getQueueId();
                    cqOffsetTable.put(key, msg.getQueueOffset() + 1);
                    startReadOffset += msg.getStoreSize();
                } finally {
                    if (lastBuffer != null)
                        lastBuffer.release();
                }

            }
        }
        //设置为topicQueueTable
        this.setTopicQueueTable(cqOffsetTable);
        //设置为BatchtopicQueueTable
        this.setBatchTopicQueueTable(bcqOffsetTable);
    }

    public void destroy() {
        for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueueInterface logic : maps.values()) {
                this.destroy(logic);
            }
        }
    }

    public void cleanExpired(long minCommitLogOffset) {
        Iterator<Map.Entry<String, ConcurrentMap<Integer, ConsumeQueueInterface>>> it = this.consumeQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, ConcurrentMap<Integer, ConsumeQueueInterface>> next = it.next();
            String topic = next.getKey();
            if (!TopicValidator.isSystemTopic(topic)) {
                ConcurrentMap<Integer, ConsumeQueueInterface> queueTable = next.getValue();
                Iterator<Map.Entry<Integer, ConsumeQueueInterface>> itQT = queueTable.entrySet().iterator();
                while (itQT.hasNext()) {
                    Map.Entry<Integer, ConsumeQueueInterface> nextQT = itQT.next();
                    long maxCLOffsetInConsumeQueue = nextQT.getValue().getLastOffset();

                    if (maxCLOffsetInConsumeQueue == -1) {
                        log.warn("maybe ConsumeQueue was created just now. topic={} queueId={} maxPhysicOffset={} minLogicOffset={}.",
                            nextQT.getValue().getTopic(),
                            nextQT.getValue().getQueueId(),
                            nextQT.getValue().getMaxPhysicOffset(),
                            nextQT.getValue().getMinLogicOffset());
                    } else if (maxCLOffsetInConsumeQueue < minCommitLogOffset) {
                        log.info(
                            "cleanExpiredConsumerQueue: {} {} consumer queue destroyed, minCommitLogOffset: {} maxCLOffsetInConsumeQueue: {}",
                            topic,
                            nextQT.getKey(),
                            minCommitLogOffset,
                            maxCLOffsetInConsumeQueue);

                        removeTopicQueueTable(nextQT.getValue().getTopic(),
                            nextQT.getValue().getQueueId());

                        this.destroy(nextQT.getValue());
                        itQT.remove();
                    }
                }

                if (queueTable.isEmpty()) {
                    log.info("cleanExpiredConsumerQueue: {},topic destroyed", topic);
                    it.remove();
                }
            }
        }
    }

    /**
     * DefaultMessageStore的方法 清除consumequeue文件中的脏数据（最大有效偏移量后面的数据）
     * @param phyOffset physical offset commitlog文件的最大有效区域的偏移量
     */
    public void truncateDirty(long phyOffset) {
        //获取consumeQueueTable遍历
        for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueueInterface logic : maps.values()) {
                this.truncateDirtyLogicFiles(logic, phyOffset);
            }
        }
    }

    public Optional<TopicConfig> getTopicConfig(String topic) {
        if (this.topicConfigTable == null) {
            return Optional.empty();
        }

        return Optional.ofNullable(this.topicConfigTable.get(topic));
    }

    public long getTotalSize() {
        long totalSize = 0;
        for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueueInterface logic : maps.values()) {
                totalSize += logic.getTotalSize();
            }
        }
        return totalSize;
    }
}
