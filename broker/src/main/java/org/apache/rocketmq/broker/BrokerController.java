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
package org.apache.rocketmq.broker;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.AccessValidator;
import org.apache.rocketmq.acl.plain.PlainAccessValidator;
import org.apache.rocketmq.broker.client.ClientHousekeepingService;
import org.apache.rocketmq.broker.client.ConsumerIdsChangeListener;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.broker.client.DefaultConsumerIdsChangeListener;
import org.apache.rocketmq.broker.client.ProducerManager;
import org.apache.rocketmq.broker.client.net.Broker2Client;
import org.apache.rocketmq.broker.client.rebalance.RebalanceLockManager;
import org.apache.rocketmq.broker.controller.ReplicasManager;
import org.apache.rocketmq.broker.dledger.DLedgerRoleChangeHandler;
import org.apache.rocketmq.broker.failover.EscapeBridge;
import org.apache.rocketmq.broker.filter.CommitLogDispatcherCalcBitMap;
import org.apache.rocketmq.broker.filter.ConsumerFilterManager;
import org.apache.rocketmq.broker.filtersrv.FilterServerManager;
import org.apache.rocketmq.broker.latency.BrokerFastFailure;
import org.apache.rocketmq.broker.latency.BrokerFixedThreadPoolExecutor;
import org.apache.rocketmq.broker.longpolling.LmqPullRequestHoldService;
import org.apache.rocketmq.broker.longpolling.NotifyMessageArrivingListener;
import org.apache.rocketmq.broker.longpolling.PullRequestHoldService;
import org.apache.rocketmq.broker.metrics.BrokerMetricsManager;
import org.apache.rocketmq.broker.mqtrace.ConsumeMessageHook;
import org.apache.rocketmq.broker.mqtrace.SendMessageHook;
import org.apache.rocketmq.broker.offset.BroadcastOffsetManager;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.broker.offset.ConsumerOrderInfoManager;
import org.apache.rocketmq.broker.offset.LmqConsumerOffsetManager;
import org.apache.rocketmq.broker.out.BrokerOuterAPI;
import org.apache.rocketmq.broker.plugin.BrokerAttachedPlugin;
import org.apache.rocketmq.broker.processor.AckMessageProcessor;
import org.apache.rocketmq.broker.processor.AdminBrokerProcessor;
import org.apache.rocketmq.broker.processor.ChangeInvisibleTimeProcessor;
import org.apache.rocketmq.broker.processor.ClientManageProcessor;
import org.apache.rocketmq.broker.processor.ConsumerManageProcessor;
import org.apache.rocketmq.broker.processor.EndTransactionProcessor;
import org.apache.rocketmq.broker.processor.NotificationProcessor;
import org.apache.rocketmq.broker.processor.PeekMessageProcessor;
import org.apache.rocketmq.broker.processor.PollingInfoProcessor;
import org.apache.rocketmq.broker.processor.PopInflightMessageCounter;
import org.apache.rocketmq.broker.processor.PopMessageProcessor;
import org.apache.rocketmq.broker.processor.PullMessageProcessor;
import org.apache.rocketmq.broker.processor.QueryAssignmentProcessor;
import org.apache.rocketmq.broker.processor.QueryMessageProcessor;
import org.apache.rocketmq.broker.processor.ReplyMessageProcessor;
import org.apache.rocketmq.broker.processor.SendMessageProcessor;
import org.apache.rocketmq.broker.schedule.ScheduleMessageService;
import org.apache.rocketmq.broker.slave.SlaveSynchronize;
import org.apache.rocketmq.broker.subscription.LmqSubscriptionGroupManager;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.broker.topic.LmqTopicConfigManager;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.broker.topic.TopicQueueMappingCleanService;
import org.apache.rocketmq.broker.topic.TopicQueueMappingManager;
import org.apache.rocketmq.broker.topic.TopicRouteInfoManager;
import org.apache.rocketmq.broker.transaction.AbstractTransactionalMessageCheckListener;
import org.apache.rocketmq.broker.transaction.TransactionalMessageCheckService;
import org.apache.rocketmq.broker.transaction.TransactionalMessageService;
import org.apache.rocketmq.broker.transaction.queue.DefaultTransactionalMessageCheckListener;
import org.apache.rocketmq.broker.transaction.queue.TransactionalMessageBridge;
import org.apache.rocketmq.broker.transaction.queue.TransactionalMessageServiceImpl;
import org.apache.rocketmq.broker.util.HookUtils;
import org.apache.rocketmq.common.AbstractBrokerRunnable;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.BrokerIdentity;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.stats.MomentStatsItem;
import org.apache.rocketmq.common.utils.ServiceProvider;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.Configuration;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.common.TlsMode;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingServer;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.netty.RequestTask;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;
import org.apache.rocketmq.remoting.protocol.BrokerSyncInfo;
import org.apache.rocketmq.remoting.protocol.DataVersion;
import org.apache.rocketmq.remoting.protocol.NamespaceUtil;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.body.BrokerMemberGroup;
import org.apache.rocketmq.remoting.protocol.body.TopicConfigAndMappingSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.namesrv.RegisterBrokerResult;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo;
import org.apache.rocketmq.srvutil.FileWatchService;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageArrivingListener;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.dledger.DLedgerCommitLog;
import org.apache.rocketmq.store.hook.PutMessageHook;
import org.apache.rocketmq.store.hook.SendMessageBackHook;
import org.apache.rocketmq.store.plugin.MessageStoreFactory;
import org.apache.rocketmq.store.plugin.MessageStorePluginContext;
import org.apache.rocketmq.store.stats.BrokerStats;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.apache.rocketmq.store.stats.LmqBrokerStatsManager;
import org.apache.rocketmq.store.timer.TimerCheckpoint;
import org.apache.rocketmq.store.timer.TimerMessageStore;
import org.apache.rocketmq.store.timer.TimerMetrics;

public class BrokerController {
    protected static final Logger LOG = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private static final Logger LOG_PROTECTION = LoggerFactory.getLogger(LoggerName.PROTECTION_LOGGER_NAME);
    private static final Logger LOG_WATER_MARK = LoggerFactory.getLogger(LoggerName.WATER_MARK_LOGGER_NAME);
    protected static final int HA_ADDRESS_MIN_LENGTH = 6;

    protected final BrokerConfig brokerConfig;
    private final NettyServerConfig nettyServerConfig;
    private final NettyClientConfig nettyClientConfig;
    protected final MessageStoreConfig messageStoreConfig;
    protected final ConsumerOffsetManager consumerOffsetManager;
    protected final BroadcastOffsetManager broadcastOffsetManager;
    protected final ConsumerManager consumerManager;
    protected final ConsumerFilterManager consumerFilterManager;
    protected final ConsumerOrderInfoManager consumerOrderInfoManager;
    protected final PopInflightMessageCounter popInflightMessageCounter;
    protected final ProducerManager producerManager;
    protected final ScheduleMessageService scheduleMessageService;
    protected final ClientHousekeepingService clientHousekeepingService;
    protected final PullMessageProcessor pullMessageProcessor;
    protected final PeekMessageProcessor peekMessageProcessor;
    protected final PopMessageProcessor popMessageProcessor;
    protected final AckMessageProcessor ackMessageProcessor;
    protected final ChangeInvisibleTimeProcessor changeInvisibleTimeProcessor;
    protected final NotificationProcessor notificationProcessor;
    protected final PollingInfoProcessor pollingInfoProcessor;
    protected final QueryAssignmentProcessor queryAssignmentProcessor;
    protected final ClientManageProcessor clientManageProcessor;
    protected final SendMessageProcessor sendMessageProcessor;
    protected final ReplyMessageProcessor replyMessageProcessor;
    protected final PullRequestHoldService pullRequestHoldService;
    protected final MessageArrivingListener messageArrivingListener;
    protected final Broker2Client broker2Client;
    protected final ConsumerIdsChangeListener consumerIdsChangeListener;
    protected final EndTransactionProcessor endTransactionProcessor;
    private final RebalanceLockManager rebalanceLockManager = new RebalanceLockManager();
    private final TopicRouteInfoManager topicRouteInfoManager;
    protected BrokerOuterAPI brokerOuterAPI;
    protected ScheduledExecutorService scheduledExecutorService;
    protected ScheduledExecutorService syncBrokerMemberGroupExecutorService;
    protected ScheduledExecutorService brokerHeartbeatExecutorService;
    protected final SlaveSynchronize slaveSynchronize;
    protected final BlockingQueue<Runnable> sendThreadPoolQueue;
    protected final BlockingQueue<Runnable> putThreadPoolQueue;
    protected final BlockingQueue<Runnable> ackThreadPoolQueue;
    protected final BlockingQueue<Runnable> pullThreadPoolQueue;
    protected final BlockingQueue<Runnable> litePullThreadPoolQueue;
    protected final BlockingQueue<Runnable> replyThreadPoolQueue;
    protected final BlockingQueue<Runnable> queryThreadPoolQueue;
    protected final BlockingQueue<Runnable> clientManagerThreadPoolQueue;
    protected final BlockingQueue<Runnable> heartbeatThreadPoolQueue;
    protected final BlockingQueue<Runnable> consumerManagerThreadPoolQueue;
    protected final BlockingQueue<Runnable> endTransactionThreadPoolQueue;
    protected final BlockingQueue<Runnable> adminBrokerThreadPoolQueue;
    protected final BlockingQueue<Runnable> loadBalanceThreadPoolQueue;
    protected final FilterServerManager filterServerManager;
    protected final BrokerStatsManager brokerStatsManager;
    protected final List<SendMessageHook> sendMessageHookList = new ArrayList<>();
    protected final List<ConsumeMessageHook> consumeMessageHookList = new ArrayList<>();
    protected MessageStore messageStore;
    protected RemotingServer remotingServer;
    protected CountDownLatch remotingServerStartLatch;
    protected RemotingServer fastRemotingServer;
    protected TopicConfigManager topicConfigManager;
    protected SubscriptionGroupManager subscriptionGroupManager;
    protected TopicQueueMappingManager topicQueueMappingManager;
    protected ExecutorService sendMessageExecutor;
    protected ExecutorService pullMessageExecutor;
    protected ExecutorService litePullMessageExecutor;
    protected ExecutorService putMessageFutureExecutor;
    protected ExecutorService ackMessageExecutor;
    protected ExecutorService replyMessageExecutor;
    protected ExecutorService queryMessageExecutor;
    protected ExecutorService adminBrokerExecutor;
    protected ExecutorService clientManageExecutor;
    protected ExecutorService heartbeatExecutor;
    protected ExecutorService consumerManageExecutor;
    protected ExecutorService loadBalanceExecutor;
    protected ExecutorService endTransactionExecutor;
    protected boolean updateMasterHAServerAddrPeriodically = false;
    private BrokerStats brokerStats;
    private InetSocketAddress storeHost;
    private TimerMessageStore timerMessageStore;
    private TimerCheckpoint timerCheckpoint;
    protected BrokerFastFailure brokerFastFailure;
    private Configuration configuration;
    protected TopicQueueMappingCleanService topicQueueMappingCleanService;
    protected FileWatchService fileWatchService;
    protected TransactionalMessageCheckService transactionalMessageCheckService;
    protected TransactionalMessageService transactionalMessageService;
    protected AbstractTransactionalMessageCheckListener transactionalMessageCheckListener;
    protected Map<Class, AccessValidator> accessValidatorMap = new HashMap<>();
    protected volatile boolean shutdown = false;
    protected ShutdownHook shutdownHook;
    private volatile boolean isScheduleServiceStart = false;
    private volatile boolean isTransactionCheckServiceStart = false;
    protected volatile BrokerMemberGroup brokerMemberGroup;
    protected EscapeBridge escapeBridge;
    protected List<BrokerAttachedPlugin> brokerAttachedPlugins = new ArrayList<>();
    protected volatile long shouldStartTime;
    private BrokerPreOnlineService brokerPreOnlineService;
    protected volatile boolean isIsolated = false;
    protected volatile long minBrokerIdInGroup = 0;
    protected volatile String minBrokerAddrInGroup = null;
    private final Lock lock = new ReentrantLock();
    protected final List<ScheduledFuture<?>> scheduledFutures = new ArrayList<>();
    protected ReplicasManager replicasManager;
    private long lastSyncTimeMs = System.currentTimeMillis();
    private BrokerMetricsManager brokerMetricsManager;

    public BrokerController(
        final BrokerConfig brokerConfig,
        final NettyServerConfig nettyServerConfig,
        final NettyClientConfig nettyClientConfig,
        final MessageStoreConfig messageStoreConfig,
        final ShutdownHook shutdownHook
    ) {
        this(brokerConfig, nettyServerConfig, nettyClientConfig, messageStoreConfig);
        this.shutdownHook = shutdownHook;
    }

    public BrokerController(
        final BrokerConfig brokerConfig,
        final MessageStoreConfig messageStoreConfig
    ) {
        this(brokerConfig, null, null, messageStoreConfig);
    }

    public BrokerController(
        final BrokerConfig brokerConfig,
        final NettyServerConfig nettyServerConfig,
        final NettyClientConfig nettyClientConfig,
        final MessageStoreConfig messageStoreConfig
    ) {
        this.brokerConfig = brokerConfig;
        this.nettyServerConfig = nettyServerConfig;
        this.nettyClientConfig = nettyClientConfig;
        this.messageStoreConfig = messageStoreConfig;
        this.setStoreHost(new InetSocketAddress(this.getBrokerConfig().getBrokerIP1(), getListenPort()));
        this.brokerStatsManager = messageStoreConfig.isEnableLmq() ? new LmqBrokerStatsManager(this.brokerConfig.getBrokerClusterName(), this.brokerConfig.isEnableDetailStat()) : new BrokerStatsManager(this.brokerConfig.getBrokerClusterName(), this.brokerConfig.isEnableDetailStat());
        this.consumerOffsetManager = messageStoreConfig.isEnableLmq() ? new LmqConsumerOffsetManager(this) : new ConsumerOffsetManager(this);
        this.broadcastOffsetManager = new BroadcastOffsetManager(this);
        this.topicConfigManager = messageStoreConfig.isEnableLmq() ? new LmqTopicConfigManager(this) : new TopicConfigManager(this);
        this.topicQueueMappingManager = new TopicQueueMappingManager(this);
        this.pullMessageProcessor = new PullMessageProcessor(this);
        this.peekMessageProcessor = new PeekMessageProcessor(this);
        this.pullRequestHoldService = messageStoreConfig.isEnableLmq() ? new LmqPullRequestHoldService(this) : new PullRequestHoldService(this);
        this.popMessageProcessor = new PopMessageProcessor(this);
        this.notificationProcessor = new NotificationProcessor(this);
        this.pollingInfoProcessor = new PollingInfoProcessor(this);
        this.ackMessageProcessor = new AckMessageProcessor(this);
        this.changeInvisibleTimeProcessor = new ChangeInvisibleTimeProcessor(this);
        this.sendMessageProcessor = new SendMessageProcessor(this);
        this.replyMessageProcessor = new ReplyMessageProcessor(this);
        this.messageArrivingListener = new NotifyMessageArrivingListener(this.pullRequestHoldService, this.popMessageProcessor, this.notificationProcessor);
        this.consumerIdsChangeListener = new DefaultConsumerIdsChangeListener(this);
        this.consumerManager = new ConsumerManager(this.consumerIdsChangeListener, this.brokerStatsManager, this.brokerConfig);
        this.producerManager = new ProducerManager(this.brokerStatsManager);
        this.consumerFilterManager = new ConsumerFilterManager(this);
        this.consumerOrderInfoManager = new ConsumerOrderInfoManager(this);
        this.popInflightMessageCounter = new PopInflightMessageCounter(this);
        this.clientHousekeepingService = new ClientHousekeepingService(this);
        this.broker2Client = new Broker2Client(this);
        this.subscriptionGroupManager = messageStoreConfig.isEnableLmq() ? new LmqSubscriptionGroupManager(this) : new SubscriptionGroupManager(this);
        this.scheduleMessageService = new ScheduleMessageService(this);

        if (nettyClientConfig != null) {
            this.brokerOuterAPI = new BrokerOuterAPI(nettyClientConfig);
        }

        this.filterServerManager = new FilterServerManager(this);

        this.queryAssignmentProcessor = new QueryAssignmentProcessor(this);
        this.clientManageProcessor = new ClientManageProcessor(this);
        this.slaveSynchronize = new SlaveSynchronize(this);
        this.endTransactionProcessor = new EndTransactionProcessor(this);

        this.sendThreadPoolQueue = new LinkedBlockingQueue<>(this.brokerConfig.getSendThreadPoolQueueCapacity());
        this.putThreadPoolQueue = new LinkedBlockingQueue<>(this.brokerConfig.getPutThreadPoolQueueCapacity());
        this.pullThreadPoolQueue = new LinkedBlockingQueue<>(this.brokerConfig.getPullThreadPoolQueueCapacity());
        this.litePullThreadPoolQueue = new LinkedBlockingQueue<>(this.brokerConfig.getLitePullThreadPoolQueueCapacity());

        this.ackThreadPoolQueue = new LinkedBlockingQueue<>(this.brokerConfig.getAckThreadPoolQueueCapacity());
        this.replyThreadPoolQueue = new LinkedBlockingQueue<>(this.brokerConfig.getReplyThreadPoolQueueCapacity());
        this.queryThreadPoolQueue = new LinkedBlockingQueue<>(this.brokerConfig.getQueryThreadPoolQueueCapacity());
        this.clientManagerThreadPoolQueue = new LinkedBlockingQueue<>(this.brokerConfig.getClientManagerThreadPoolQueueCapacity());
        this.consumerManagerThreadPoolQueue = new LinkedBlockingQueue<>(this.brokerConfig.getConsumerManagerThreadPoolQueueCapacity());
        this.heartbeatThreadPoolQueue = new LinkedBlockingQueue<>(this.brokerConfig.getHeartbeatThreadPoolQueueCapacity());
        this.endTransactionThreadPoolQueue = new LinkedBlockingQueue<>(this.brokerConfig.getEndTransactionPoolQueueCapacity());
        this.adminBrokerThreadPoolQueue = new LinkedBlockingQueue<>(this.brokerConfig.getAdminBrokerThreadPoolQueueCapacity());
        this.loadBalanceThreadPoolQueue = new LinkedBlockingQueue<>(this.brokerConfig.getLoadBalanceThreadPoolQueueCapacity());

        this.brokerFastFailure = new BrokerFastFailure(this);

        String brokerConfigPath;
        if (brokerConfig.getBrokerConfigPath() != null && !brokerConfig.getBrokerConfigPath().isEmpty()) {
            brokerConfigPath = brokerConfig.getBrokerConfigPath();
        } else {
            brokerConfigPath = FilenameUtils.concat(
                FilenameUtils.getFullPathNoEndSeparator(BrokerPathConfigHelper.getBrokerConfigPath()),
                this.brokerConfig.getCanonicalName() + ".properties");
        }
        this.configuration = new Configuration(
            LOG,
            brokerConfigPath,
            this.brokerConfig, this.nettyServerConfig, this.nettyClientConfig, this.messageStoreConfig
        );

        this.brokerStatsManager.setProduerStateGetter(new BrokerStatsManager.StateGetter() {
            @Override
            public boolean online(String instanceId, String group, String topic) {
                if (getTopicConfigManager().getTopicConfigTable().containsKey(NamespaceUtil.wrapNamespace(instanceId, topic))) {
                    return getProducerManager().groupOnline(NamespaceUtil.wrapNamespace(instanceId, group));
                } else {
                    return getProducerManager().groupOnline(group);
                }
            }
        });
        this.brokerStatsManager.setConsumerStateGetter(new BrokerStatsManager.StateGetter() {
            @Override
            public boolean online(String instanceId, String group, String topic) {
                String topicFullName = NamespaceUtil.wrapNamespace(instanceId, topic);
                if (getTopicConfigManager().getTopicConfigTable().containsKey(topicFullName)) {
                    return getConsumerManager().findSubscriptionData(NamespaceUtil.wrapNamespace(instanceId, group), topicFullName) != null;
                } else {
                    return getConsumerManager().findSubscriptionData(group, topic) != null;
                }
            }
        });

        this.brokerMemberGroup = new BrokerMemberGroup(this.brokerConfig.getBrokerClusterName(), this.brokerConfig.getBrokerName());
        this.brokerMemberGroup.getBrokerAddrs().put(this.brokerConfig.getBrokerId(), this.getBrokerAddr());

        this.escapeBridge = new EscapeBridge(this);

        this.topicRouteInfoManager = new TopicRouteInfoManager(this);

        if (this.brokerConfig.isEnableSlaveActingMaster() && !this.brokerConfig.isSkipPreOnline()) {
            this.brokerPreOnlineService = new BrokerPreOnlineService(this);
        }
    }

    public BrokerConfig getBrokerConfig() {
        return brokerConfig;
    }

    public NettyServerConfig getNettyServerConfig() {
        return nettyServerConfig;
    }

    public NettyClientConfig getNettyClientConfig() {
        return nettyClientConfig;
    }

    public BlockingQueue<Runnable> getPullThreadPoolQueue() {
        return pullThreadPoolQueue;
    }

    public BlockingQueue<Runnable> getQueryThreadPoolQueue() {
        return queryThreadPoolQueue;
    }

    public BrokerMetricsManager getBrokerMetricsManager() {
        return brokerMetricsManager;
    }

    /**
     * 初始化Broker通信层
     * 在配置文件和各种日志文件加载、恢复完毕之后，开始初始化Broker通信层和各种请求执行器。
     * 首先会创建两个NettyRemotingServer，分别是普通的远程服务remotingServer，默认端口为10911，以及快速远程服务fastRemotingServer，端口号是普通服务的端口号-2，默认10909
     */
    protected void initializeRemotingServer() throws CloneNotSupportedException {
        //创建broker的netty远程服务，端口为10911，可以用于处理客户端的所有请求。
        this.remotingServer = new NettyRemotingServer(this.nettyServerConfig, this.clientHousekeepingService);
        //创建一个broker的netty快速远程服务的配置
        NettyServerConfig fastConfig = (NettyServerConfig) this.nettyServerConfig.clone();
        //设置快速netty远程服务的配置监听的端口号为普通服务的端口-2，默认10909
        int listeningPort = nettyServerConfig.getListenPort() - 2;
        if (listeningPort < 0) {
            listeningPort = 0;
        }
        fastConfig.setListenPort(listeningPort);
        //创建broker的netty快速服务，这就是所谓的快速通道，对应可以处理客户端除了拉取消息之外的所有请求，所谓的VIP端口。
        this.fastRemotingServer = new NettyRemotingServer(fastConfig, this.clientHousekeepingService);
    }

    /**
     * 创建各种执行器线程池
     * Initialize resources including remoting server and thread executors.
     */
    protected void initializeResources() {
        //定时任务线程池
        //在注册了netty消息处理器之后，将会启动一系列的定时任务。这些定时任务由BrokerController中的scheduledExecutorService去执行，该线程池只有一个线程。
        this.scheduledExecutorService = new ScheduledThreadPoolExecutor(1,
            new ThreadFactoryImpl("BrokerControllerScheduledThread", true, getBrokerIdentity()));

        //处理发送消息的请求的线程池
        this.sendMessageExecutor = new BrokerFixedThreadPoolExecutor(
            this.brokerConfig.getSendMessageThreadPoolNums(),
            this.brokerConfig.getSendMessageThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.sendThreadPoolQueue,
            new ThreadFactoryImpl("SendMessageThread_", getBrokerIdentity()));

        //处理拉取消息的请求的线程池
        this.pullMessageExecutor = new BrokerFixedThreadPoolExecutor(
            this.brokerConfig.getPullMessageThreadPoolNums(),
            this.brokerConfig.getPullMessageThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.pullThreadPoolQueue,
            new ThreadFactoryImpl("PullMessageThread_", getBrokerIdentity()));

        //处理拉取消息的请求的线程池
        this.litePullMessageExecutor = new BrokerFixedThreadPoolExecutor(
            this.brokerConfig.getLitePullMessageThreadPoolNums(),
            this.brokerConfig.getLitePullMessageThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.litePullThreadPoolQueue,
            new ThreadFactoryImpl("LitePullMessageThread_", getBrokerIdentity()));

        //https://github.com/apache/rocketmq/pull/3631
        this.putMessageFutureExecutor = new BrokerFixedThreadPoolExecutor(
            this.brokerConfig.getPutMessageFutureThreadPoolNums(),
            this.brokerConfig.getPutMessageFutureThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.putThreadPoolQueue,
            new ThreadFactoryImpl("SendMessageThread_", getBrokerIdentity()));

        //消息确认线程池
        this.ackMessageExecutor = new BrokerFixedThreadPoolExecutor(
            this.brokerConfig.getAckMessageThreadPoolNums(),
            this.brokerConfig.getAckMessageThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.ackThreadPoolQueue,
            new ThreadFactoryImpl("AckMessageThread_", getBrokerIdentity()));

        //处理查询请求的线程池
        this.queryMessageExecutor = new BrokerFixedThreadPoolExecutor(
            this.brokerConfig.getQueryMessageThreadPoolNums(),
            this.brokerConfig.getQueryMessageThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.queryThreadPoolQueue,
            new ThreadFactoryImpl("QueryMessageThread_", getBrokerIdentity()));

        //broker 管理线程池，作为默认处理器的线程池
        this.adminBrokerExecutor = new BrokerFixedThreadPoolExecutor(
            this.brokerConfig.getAdminBrokerThreadPoolNums(),
            this.brokerConfig.getAdminBrokerThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.adminBrokerThreadPoolQueue,
            new ThreadFactoryImpl("AdminBrokerThread_", getBrokerIdentity()));

        //客户端管理器的线程池
        this.clientManageExecutor = new BrokerFixedThreadPoolExecutor(
            this.brokerConfig.getClientManageThreadPoolNums(),
            this.brokerConfig.getClientManageThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.clientManagerThreadPoolQueue,
            new ThreadFactoryImpl("ClientManageThread_", getBrokerIdentity()));

        //心跳处理的线程池
        this.heartbeatExecutor = new BrokerFixedThreadPoolExecutor(
            this.brokerConfig.getHeartbeatThreadPoolNums(),
            this.brokerConfig.getHeartbeatThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.heartbeatThreadPoolQueue,
            new ThreadFactoryImpl("HeartbeatThread_", true, getBrokerIdentity()));

        //消费者管理的线程池
        this.consumerManageExecutor = new BrokerFixedThreadPoolExecutor(
            this.brokerConfig.getConsumerManageThreadPoolNums(),
            this.brokerConfig.getConsumerManageThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.consumerManagerThreadPoolQueue,
            new ThreadFactoryImpl("ConsumerManageThread_", true, getBrokerIdentity()));

        //处理reply消息的请求的线程池
        this.replyMessageExecutor = new BrokerFixedThreadPoolExecutor(
            this.brokerConfig.getProcessReplyMessageThreadPoolNums(),
            this.brokerConfig.getProcessReplyMessageThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.replyThreadPoolQueue,
            new ThreadFactoryImpl("ProcessReplyMessageThread_", getBrokerIdentity()));

        //事务消息相关处理的线程池
        this.endTransactionExecutor = new BrokerFixedThreadPoolExecutor(
            this.brokerConfig.getEndTransactionThreadPoolNums(),
            this.brokerConfig.getEndTransactionThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.endTransactionThreadPoolQueue,
            new ThreadFactoryImpl("EndTransactionThread_", getBrokerIdentity()));

        //负载均衡线程池
        this.loadBalanceExecutor = new BrokerFixedThreadPoolExecutor(
            this.brokerConfig.getLoadBalanceProcessorThreadPoolNums(),
            this.brokerConfig.getLoadBalanceProcessorThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.loadBalanceThreadPoolQueue,
            new ThreadFactoryImpl("LoadBalanceProcessorThread_", getBrokerIdentity()));
        //broker同步线程池
        this.syncBrokerMemberGroupExecutorService = new ScheduledThreadPoolExecutor(1,
            new ThreadFactoryImpl("BrokerControllerSyncBrokerScheduledThread", getBrokerIdentity()));
        //broker心跳处理的线程池
        this.brokerHeartbeatExecutorService = new ScheduledThreadPoolExecutor(1,
            new ThreadFactoryImpl("BrokerControllerHeartbeatScheduledThread", getBrokerIdentity()));
        //清理topic队列
        this.topicQueueMappingCleanService = new TopicQueueMappingCleanService(this);
    }

    /**
     * 启动一系列定时周期任务
     */
    protected void initializeBrokerScheduledTasks() {
        //每隔24h打印昨天生产和消费的消息数量
        final long initialDelay = UtilAll.computeNextMorningTimeMillis() - System.currentTimeMillis();
        final long period = TimeUnit.DAYS.toMillis(1);
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    BrokerController.this.getBrokerStats().record();
                } catch (Throwable e) {
                    LOG.error("BrokerController: failed to record broker stats", e);
                }
            }
        }, initialDelay, period, TimeUnit.MILLISECONDS);

        //每隔5s將消费者offset进行持久化，存入consumerOffset.json文件中
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    BrokerController.this.consumerOffsetManager.persist();
                } catch (Throwable e) {
                    LOG.error(
                        "BrokerController: failed to persist config file of consumerOffset", e);
                }
            }
        }, 1000 * 10, this.brokerConfig.getFlushConsumerOffsetInterval(), TimeUnit.MILLISECONDS);

        //每隔10s將消费过滤信息进行持久化，存入consumerFilter.json文件中
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    BrokerController.this.consumerFilterManager.persist();
                    BrokerController.this.consumerOrderInfoManager.persist();
                } catch (Throwable e) {
                    LOG.error(
                        "BrokerController: failed to persist config file of consumerFilter or consumerOrderInfo",
                        e);
                }
            }
        }, 1000 * 10, 1000 * 10, TimeUnit.MILLISECONDS);

        //每隔3m將检查消费者的消费进度
        //当消费进度落后阈值的时候，并且disableConsumeIfConsumerReadSlowly=true(默认false)，就停止消费者消费，保护broker，避免消费积压
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    BrokerController.this.protectBroker();
                } catch (Throwable e) {
                    LOG.error("BrokerController: failed to protectBroker", e);
                }
            }
        }, 3, 3, TimeUnit.MINUTES);

        //每隔1s將打印发送消息线程池队列、拉取消息线程池队列、查询消息线程池队列、结束事务线程池队列的大小以及队列头部元素存在时间
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    BrokerController.this.printWaterMark();
                } catch (Throwable e) {
                    LOG.error("BrokerController: failed to print broker watermark", e);
                }
            }
        }, 10, 1, TimeUnit.SECONDS);

        //每隔1m將打印已存储在commitlog提交日志中但尚未分派到consume queue消费队列的字节数。
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    LOG.info("Dispatch task fall behind commit log {}bytes",
                        BrokerController.this.getMessageStore().dispatchBehindBytes());
                } catch (Throwable e) {
                    LOG.error("Failed to print dispatchBehindBytes", e);
                }
            }
        }, 1000 * 10, 1000 * 60, TimeUnit.MILLISECONDS);

        //todo 问题
        //如果没有开启DLeger服务，DLeger开启后表示支持高可用的主从自动切换
        if (!messageStoreConfig.isEnableDLegerCommitLog() && !messageStoreConfig.isDuplicationEnable() && !brokerConfig.isEnableControllerMode()) {
            //如果当前broker是slave从节点
            if (BrokerRole.SLAVE == this.messageStoreConfig.getBrokerRole()) {
                //根据是否配置了HA地址，来更新HA地址，并设置updateMasterHAServerAddrPeriodically
                if (this.messageStoreConfig.getHaMasterAddress() != null && this.messageStoreConfig.getHaMasterAddress().length() >= HA_ADDRESS_MIN_LENGTH) {
                    this.messageStore.updateHaMasterAddress(this.messageStoreConfig.getHaMasterAddress());
                    this.updateMasterHAServerAddrPeriodically = false;
                } else {
                    this.updateMasterHAServerAddrPeriodically = true;
                }

                //todo 问题
                //如果没有指定nameServer地址，并且允许从地址服务器获取nameServer地址
                //那么每隔2m从nameServer地址服务器拉取最新的nameServer地址并更新
                //要想动态更新nameServer地址，需要指定一个地址服务器的url，并且fetchNamesrvAddrByAddressServer设置为true
                this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            if (System.currentTimeMillis() - lastSyncTimeMs > 60 * 1000) {
                                BrokerController.this.getSlaveSynchronize().syncAll();
                                lastSyncTimeMs = System.currentTimeMillis();
                            }
                            //timer checkpoint, latency-sensitive, so sync it more frequently
                            BrokerController.this.getSlaveSynchronize().syncTimerCheckPoint();
                        } catch (Throwable e) {
                            LOG.error("Failed to sync all config for slave.", e);
                        }
                    }
                }, 1000 * 10, 3 * 1000, TimeUnit.MILLISECONDS);

            } else {
                this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                    //如果是主节点，每隔60s將打印主从节点的差异
                    @Override
                    public void run() {
                        try {
                            BrokerController.this.printMasterAndSlaveDiff();
                        } catch (Throwable e) {
                            LOG.error("Failed to print diff of master and slave.", e);
                        }
                    }
                }, 1000 * 10, 1000 * 60, TimeUnit.MILLISECONDS);
            }
        }

        if (this.brokerConfig.isEnableControllerMode()) {
            this.updateMasterHAServerAddrPeriodically = true;
        }
    }

    /**
     * 启动一系列定时周期任务
     */
    protected void initializeScheduledTasks() {

        /*
         * 初始化周期任务
         */
        initializeBrokerScheduledTasks();

        //刷新元数据信息
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    BrokerController.this.brokerOuterAPI.refreshMetadata();
                } catch (Exception e) {
                    LOG.error("ScheduledTask refresh metadata exception", e);
                }
            }
        }, 10, 5, TimeUnit.SECONDS);

        //如果broker的nameServer地址不为null
        if (this.brokerConfig.getNamesrvAddr() != null) {

            //更新nameServer地址
            this.updateNamesrvAddr();
            LOG.info("Set user specified name server address: {}", this.brokerConfig.getNamesrvAddr());
            // also auto update namesrv if specify
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        BrokerController.this.updateNamesrvAddr();
                    } catch (Throwable e) {
                        LOG.error("Failed to update nameServer address list", e);
                    }
                }
            }, 1000 * 10, 1000 * 60 * 2, TimeUnit.MILLISECONDS);
        } else if (this.brokerConfig.isFetchNamesrvAddrByAddressServer()) {
            //如果没有指定nameServer地址，并且允许从地址服务器获取nameServer地址
            //那么每隔2m从nameServer地址服务器拉取最新的nameServer地址并更新
            //要想动态更新nameServer地址，需要指定一个地址服务器的url，并且fetchNamesrvAddrByAddressServer设置为true
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

                @Override
                public void run() {
                    try {
                        BrokerController.this.brokerOuterAPI.fetchNameServerAddr();
                    } catch (Throwable e) {
                        LOG.error("Failed to fetch nameServer address", e);
                    }
                }
            }, 1000 * 10, 1000 * 60 * 2, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * 更新nameServer地址
     */
    private void updateNamesrvAddr() {
        if (this.brokerConfig.isFetchNameSrvAddrByDnsLookup()) {
            this.brokerOuterAPI.updateNameServerAddressListByDnsLookup(this.brokerConfig.getNamesrvAddr());
        } else {
            this.brokerOuterAPI.updateNameServerAddressList(this.brokerConfig.getNamesrvAddr());
        }
    }

    /**
     * 初始化BrokerColtroller
     * @return
     * @throws CloneNotSupportedException
     */
    public boolean initialize() throws CloneNotSupportedException {
        /*
         * 1 加载配置文件
         * 尝试从json配置文件或者bak备份文件中加载json字符串，然后反序列化转换为自身内部的属性
         */
        //topic配置文件加载，路径为  {user.home}/store/config/topics.json
        boolean result = this.topicConfigManager.load();
        //消费者消费偏移量配置文件加载，路径为  {user.home}/store/config/consumerOffset.json
        result = result && this.topicQueueMappingManager.load();
        //订阅分组配置文件加载，路径为  {user.home}/store/config/subscriptionGroup.json
        result = result && this.consumerOffsetManager.load();
        result = result && this.subscriptionGroupManager.load();
        //消费者过滤配置文件加载，路径为  {user.home}/store/config/consumerFilter.json
        result = result && this.consumerFilterManager.load();
        result = result && this.consumerOrderInfoManager.load();

        /*
         * 如果上一步加载配置全部成功
         * 2 实例化和初始化消息存储服务相关类DefaultMessageStore
         */
        if (result) {
            try {
                //实例化消息存储类DefaultMessageStore
                DefaultMessageStore defaultMessageStore = new DefaultMessageStore(this.messageStoreConfig, this.brokerStatsManager, this.messageArrivingListener, this.brokerConfig);
                defaultMessageStore.setTopicConfigTable(topicConfigManager.getTopicConfigTable());

                //如果启动了enableDLegerCommitLog，就创建DLeger组件DLedgerRoleChangeHandler。默认为false，如果需要开启则该值需要设置为true。
                //在启用enableDLegerCommitLog情况下，broker通过raft协议选主，可以实现主从角色自动切换，这是4.5版本之后的新功能
                //简单的说，如果启动enableDLegerCommitLog，就表示启用 RocketMQ 的容灾机制——自动主从切换，
                if (messageStoreConfig.isEnableDLegerCommitLog()) {
                    DLedgerRoleChangeHandler roleChangeHandler = new DLedgerRoleChangeHandler(this, defaultMessageStore);
                    ((DLedgerCommitLog) defaultMessageStore.getCommitLog()).getdLedgerServer().getDLedgerLeaderElector().addRoleChangeHandler(roleChangeHandler);
                }
                //broker的统计服务类，保存了broker的一些统计数据
                //例如msgPutTotalTodayNow --现在存储的消息数量，msgPutTotalTodayMorning --今天存储的消息数量
                this.brokerStats = new BrokerStats(defaultMessageStore);
                //load plugin
                //加载存在的消息存储插件，不必深究
                MessageStorePluginContext context = new MessageStorePluginContext(messageStoreConfig, brokerStatsManager, messageArrivingListener, brokerConfig, configuration);
                this.messageStore = MessageStoreFactory.build(context, defaultMessageStore);
                //添加一个针对布隆过滤器的消费过滤类
                this.messageStore.getDispatcherList().addFirst(new CommitLogDispatcherCalcBitMap(this.brokerConfig, this.consumerFilterManager));
                if (this.brokerConfig.isEnableControllerMode()) {
                    this.replicasManager = new ReplicasManager(this);
                }
                if (messageStoreConfig.isTimerWheelEnable()) {
                    this.timerCheckpoint = new TimerCheckpoint(BrokerPathConfigHelper.getTimerCheckPath(messageStoreConfig.getStorePathRootDir()));
                    TimerMetrics timerMetrics = new TimerMetrics(BrokerPathConfigHelper.getTimerMetricsPath(messageStoreConfig.getStorePathRootDir()));
                    this.timerMessageStore = new TimerMessageStore(messageStore, messageStoreConfig, timerCheckpoint, timerMetrics, brokerStatsManager);
                    this.timerMessageStore.registerEscapeBridgeHook(msg -> escapeBridge.putMessage(msg));
                    this.messageStore.setTimerMessageStore(this.timerMessageStore);
                }
            } catch (IOException e) {
                result = false;
                LOG.error("BrokerController#initialize: unexpected error occurs", e);
            }
        }

        /*
         * 如果上一步加载配置全部成功
         * 通过消息存储服务加载消息存储的相关文件，比如commitLog日志文件、consumequeue消息消费队列文件的加载，indexFile索引文件的构建
         * messageStore还会将这些文件的内容加载到内存中，并且完成RocketMQ的数据恢复
         * 这是核broker启动的心步骤之一
         */
        if (messageStore != null) {
            registerMessageStoreHook();
            result = result && this.messageStore.load();
        }

        /*
         * 是否开启时间轮
         */
        if (messageStoreConfig.isTimerWheelEnable()) {
            result = result && this.timerMessageStore.load();
        }

        //scheduleMessageService load after messageStore load success
        result = result && this.scheduleMessageService.load();

        for (BrokerAttachedPlugin brokerAttachedPlugin : brokerAttachedPlugins) {
            if (brokerAttachedPlugin != null) {
                result = result && brokerAttachedPlugin.load();
            }
        }

        this.brokerMetricsManager = new BrokerMetricsManager(this);

        if (result) {
            /*
             * 创建netty远程服务，remotingServer和fastRemotingServer
             * 初始化Broker通信层
             * 在配置文件和各种日志文件加载、恢复完毕之后，开始初始化Broker通信层和各种请求执行器。
             * 首先会创建两个NettyRemotingServer，分别是普通的远程服务remotingServer，默认端口为10911，以及快速远程服务fastRemotingServer，端口号是普通服务的端口号-2，默认10909
             * 在注册了netty消息处理器之后，将会启动一系列的定时任务。这些定时任务由BrokerController中的scheduledExecutorService去执行，该线程池只有一个线程。
             */
            //创建broker的netty远程服务，端口为10911，可以用于处理客户端的所有请求。

            initializeRemotingServer();
            /*
             * 创建各种执行器线程池
             * 创建很多个线程池，因为RocketMQ为了性能，将许多的请求的进行异步化处理，因此需要多个线程池。
             */
            initializeResources();

            /*
             * 注册处理器
             * RocketMQ底层通信基于netty，这里注册netty请求处理器。当对应类型的请求到达broker服务端的时候，便会由对应的消息处理器进行处理。
             */
            registerProcessor();

            /*
             * 初始化周期任务
             */
            initializeScheduledTasks();

            /*
             * 初始化事务消息相关服务
             */
            initialTransaction();

            /*
             * 初始化权限相关服务
             */
            initialAcl();

            /*
             * 初始化RPC调用的钩子函数
             */
            initialRpcHooks();

            //Tls传输相关配置，通信安全的文件监听模块，用来观察网络加密配置文件的更改
            //默认是PERMISSIVE，因此会进入代码块
            if (TlsSystemConfig.tlsMode != TlsMode.DISABLED) {
                // Register a listener to reload SslContext
                try {
                    //实例化文件监听服务,并且初始化事务消息服务。
                    fileWatchService = new FileWatchService(
                        new String[] {
                            TlsSystemConfig.tlsServerCertPath,
                            TlsSystemConfig.tlsServerKeyPath,
                            TlsSystemConfig.tlsServerTrustCertPath
                        },
                        new FileWatchService.Listener() {
                            boolean certChanged, keyChanged = false;

                            @Override
                            public void onChanged(String path) {
                                if (path.equals(TlsSystemConfig.tlsServerTrustCertPath)) {
                                    LOG.info("The trust certificate changed, reload the ssl context");
                                    reloadServerSslContext();
                                }
                                if (path.equals(TlsSystemConfig.tlsServerCertPath)) {
                                    certChanged = true;
                                }
                                if (path.equals(TlsSystemConfig.tlsServerKeyPath)) {
                                    keyChanged = true;
                                }
                                if (certChanged && keyChanged) {
                                    LOG.info("The certificate and private key changed, reload the ssl context");
                                    certChanged = keyChanged = false;
                                    reloadServerSslContext();
                                }
                            }

                            private void reloadServerSslContext() {
                                ((NettyRemotingServer) remotingServer).loadSslContext();
                                ((NettyRemotingServer) fastRemotingServer).loadSslContext();
                            }
                        });
                } catch (Exception e) {
                    result = false;
                    LOG.warn("FileWatchService created error, can't load the certificate dynamically");
                }
            }
        }

        return result;
    }

    public void registerMessageStoreHook() {
        List<PutMessageHook> putMessageHookList = messageStore.getPutMessageHookList();

        putMessageHookList.add(new PutMessageHook() {
            @Override
            public String hookName() {
                return "checkBeforePutMessage";
            }

            @Override
            public PutMessageResult executeBeforePutMessage(MessageExt msg) {
                return HookUtils.checkBeforePutMessage(BrokerController.this, msg);
            }
        });

        putMessageHookList.add(new PutMessageHook() {
            @Override
            public String hookName() {
                return "innerBatchChecker";
            }

            @Override
            public PutMessageResult executeBeforePutMessage(MessageExt msg) {
                if (msg instanceof MessageExtBrokerInner) {
                    return HookUtils.checkInnerBatch(BrokerController.this, msg);
                }
                return null;
            }
        });

        putMessageHookList.add(new PutMessageHook() {
            @Override
            public String hookName() {
                return "handleScheduleMessage";
            }

            @Override
            public PutMessageResult executeBeforePutMessage(MessageExt msg) {
                if (msg instanceof MessageExtBrokerInner) {
                    return HookUtils.handleScheduleMessage(BrokerController.this, (MessageExtBrokerInner) msg);
                }
                return null;
            }
        });

        SendMessageBackHook sendMessageBackHook = new SendMessageBackHook() {
            @Override
            public boolean executeSendMessageBack(List<MessageExt> msgList, String brokerName, String brokerAddr) {
                return HookUtils.sendMessageBack(BrokerController.this, msgList, brokerName, brokerAddr);
            }
        };

        if (messageStore != null) {
            messageStore.setSendMessageBackHook(sendMessageBackHook);
        }
    }

    /**
     * initialTransaction方法用于初始化事务消息相关服务。
     * 事务消息的服务采用Java SPI的方式进行加载。
     */
    private void initialTransaction() {
        //基于Java的SPI机制，查找"META-INF/service/org.apache.rocketmq.broker.transaction.TransactionalMessageService"文件里面的SPI实现
        //事务消息服务
        this.transactionalMessageService = ServiceProvider.loadClass(TransactionalMessageService.class);
        if (null == this.transactionalMessageService) {
            //如果沒有通过SPI指定具体的实现，那么使用默认实现，TransactionalMessageServiceImpl
            this.transactionalMessageService = new TransactionalMessageServiceImpl(
                    new TransactionalMessageBridge(this, this.getMessageStore()));
            LOG.warn("Load default transaction message hook service: {}",
                    TransactionalMessageServiceImpl.class.getSimpleName());
        }
        //基于Java的SPI机制，查找"META-INF/service/org.apache.rocketmq.broker.transaction.AbstractTransactionalMessageCheckListener"文件里面的SPI实现
        //事务消息回查服务监听器
        this.transactionalMessageCheckListener = ServiceProvider.loadClass(
                AbstractTransactionalMessageCheckListener.class);
        if (null == this.transactionalMessageCheckListener) {
            //如果沒有通过SPI指定具体的实现，那么使用默认实现，DefaultTransactionalMessageCheckListener
            this.transactionalMessageCheckListener = new DefaultTransactionalMessageCheckListener();
            LOG.warn("Load default discard message hook service: {}",
                    DefaultTransactionalMessageCheckListener.class.getSimpleName());
        }
        this.transactionalMessageCheckListener.setBrokerController(this);
        //创建TransactionalMessageCheckService服务，该服务内部有一个线程
        //会定时每一分钟(可通过在broker.conf文件设置transactionCheckInterval属性更改)触发事务检查的逻辑，内部调用TransactionalMessageService#check方法
        //默认情况下，6秒以上没commit/rollback的事务消息才会触发事务回查，而如果回查次数超过15次则丢弃事务
        this.transactionalMessageCheckService = new TransactionalMessageCheckService(this);
    }

    /**
     * BrokerController的方法
     * 初始化ACL权限校验器
     */
    private void initialAcl() {
        //校验是否开启了ACL，默认false，所以直接返回了
        if (!this.brokerConfig.isAclEnable()) {
            LOG.info("The broker dose not enable acl");
            return;
        }
        //如果开启了ACL，则首先通过SPI机制获取AccessValidator
        List<AccessValidator> accessValidators = ServiceProvider.load(AccessValidator.class);
        if (accessValidators.isEmpty()) {
            LOG.info("ServiceProvider loaded no AccessValidator, using default org.apache.rocketmq.acl.plain.PlainAccessValidator");
            accessValidators.add(new PlainAccessValidator());
        }
        //将校验器存入accessValidatorMap，并且注册到RpcHook中，在请求之前会执行校验。
        for (AccessValidator accessValidator : accessValidators) {
            final AccessValidator validator = accessValidator;
            accessValidatorMap.put(validator.getClass(), validator);
            this.registerServerRPCHook(new RPCHook() {

                @Override
                public void doBeforeRequest(String remoteAddr, RemotingCommand request) {
                    //Do not catch the exception
                    //在执行请求之前会进行校验
                    validator.validate(validator.parse(request, remoteAddr));
                }

                @Override
                public void doAfterResponse(String remoteAddr, RemotingCommand request, RemotingCommand response) {
                }

            });
        }
    }
    /**
     * BrokerController的方法
     * 初始化RpcHook服务
     */
    private void initialRpcHooks() {
        //通过SPI机制获取RPCHook的实现
        List<RPCHook> rpcHooks = ServiceProvider.load(RPCHook.class);
        //如果没有配置RpcHook，那么直接返回
        if (rpcHooks == null || rpcHooks.isEmpty()) {
            return;
        }
        //遍历并且注册所有的RpcHook
        for (RPCHook rpcHook : rpcHooks) {
            this.registerServerRPCHook(rpcHook);
        }
    }

    /**
     * BrokerController的方法
     * 注册netty消息处理器
     * RocketMQ底层通信基于netty，这里注册netty请求处理器。当对应类型的请求到达broker服务端的时候，便会由对应的消息处理器进行处理。
     * 该方法将会注册SendMessageProcessor、pullMessageExecutor、ReplyMessageProcessor、QueryMessageProcessor、
     * ClientManageProcessor、ConsumerManageProcessor、EndTransactionProcessor、AdminBrokerProcessor这几个处理器。
     * 除了pullMessageProcessor处理器只会被注册到remotingServer之外，
     * 其他处理器会被注册到remotingServer和fastRemotingServer这两个netty服务中。从这里的源码能够看出来，Vip通道服务不能够处理拉取消息的请求。
     */
    public void registerProcessor() {
        /*
         * 发送消息处理器
         * SendMessageProcessor
         */
        sendMessageProcessor.registerSendMessageHook(sendMessageHookList);
        sendMessageProcessor.registerConsumeMessageHook(consumeMessageHookList);

        //对于发送类型的请求，使用发送消息处理器sendProcessor来处理
        this.remotingServer.registerProcessor(RequestCode.SEND_MESSAGE, sendMessageProcessor, this.sendMessageExecutor);
        this.remotingServer.registerProcessor(RequestCode.SEND_MESSAGE_V2, sendMessageProcessor, this.sendMessageExecutor);
        this.remotingServer.registerProcessor(RequestCode.SEND_BATCH_MESSAGE, sendMessageProcessor, this.sendMessageExecutor);
        this.remotingServer.registerProcessor(RequestCode.CONSUMER_SEND_MSG_BACK, sendMessageProcessor, this.sendMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.SEND_MESSAGE, sendMessageProcessor, this.sendMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.SEND_MESSAGE_V2, sendMessageProcessor, this.sendMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.SEND_BATCH_MESSAGE, sendMessageProcessor, this.sendMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.CONSUMER_SEND_MSG_BACK, sendMessageProcessor, this.sendMessageExecutor);
        /**
         * PullMessageProcessor
         */
        this.remotingServer.registerProcessor(RequestCode.PULL_MESSAGE, this.pullMessageProcessor, this.pullMessageExecutor);
        this.remotingServer.registerProcessor(RequestCode.LITE_PULL_MESSAGE, this.pullMessageProcessor, this.litePullMessageExecutor);
        this.pullMessageProcessor.registerConsumeMessageHook(consumeMessageHookList);
        /**
         * PeekMessageProcessor
         */
        this.remotingServer.registerProcessor(RequestCode.PEEK_MESSAGE, this.peekMessageProcessor, this.pullMessageExecutor);
        /**
         * PopMessageProcessor
         */
        this.remotingServer.registerProcessor(RequestCode.POP_MESSAGE, this.popMessageProcessor, this.pullMessageExecutor);

        /**
         * AckMessageProcessor
         */
        this.remotingServer.registerProcessor(RequestCode.ACK_MESSAGE, this.ackMessageProcessor, this.ackMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.ACK_MESSAGE, this.ackMessageProcessor, this.ackMessageExecutor);
        /**
         * ChangeInvisibleTimeProcessor
         */
        this.remotingServer.registerProcessor(RequestCode.CHANGE_MESSAGE_INVISIBLETIME, this.changeInvisibleTimeProcessor, this.ackMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.CHANGE_MESSAGE_INVISIBLETIME, this.changeInvisibleTimeProcessor, this.ackMessageExecutor);
        /**
         * notificationProcessor
         */
        this.remotingServer.registerProcessor(RequestCode.NOTIFICATION, this.notificationProcessor, this.pullMessageExecutor);

        /**
         * pollingInfoProcessor
         */
        this.remotingServer.registerProcessor(RequestCode.POLLING_INFO, this.pollingInfoProcessor, this.pullMessageExecutor);

        /**
         * ReplyMessageProcessor
         */

        replyMessageProcessor.registerSendMessageHook(sendMessageHookList);

        this.remotingServer.registerProcessor(RequestCode.SEND_REPLY_MESSAGE, replyMessageProcessor, replyMessageExecutor);
        this.remotingServer.registerProcessor(RequestCode.SEND_REPLY_MESSAGE_V2, replyMessageProcessor, replyMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.SEND_REPLY_MESSAGE, replyMessageProcessor, replyMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.SEND_REPLY_MESSAGE_V2, replyMessageProcessor, replyMessageExecutor);

        /**
         * QueryMessageProcessor
         */
        NettyRequestProcessor queryProcessor = new QueryMessageProcessor(this);
        this.remotingServer.registerProcessor(RequestCode.QUERY_MESSAGE, queryProcessor, this.queryMessageExecutor);
        this.remotingServer.registerProcessor(RequestCode.VIEW_MESSAGE_BY_ID, queryProcessor, this.queryMessageExecutor);

        this.fastRemotingServer.registerProcessor(RequestCode.QUERY_MESSAGE, queryProcessor, this.queryMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.VIEW_MESSAGE_BY_ID, queryProcessor, this.queryMessageExecutor);

        /**
         * ClientManageProcessor
         */
        this.remotingServer.registerProcessor(RequestCode.HEART_BEAT, clientManageProcessor, this.heartbeatExecutor);
        this.remotingServer.registerProcessor(RequestCode.UNREGISTER_CLIENT, clientManageProcessor, this.clientManageExecutor);
        this.remotingServer.registerProcessor(RequestCode.CHECK_CLIENT_CONFIG, clientManageProcessor, this.clientManageExecutor);

        this.fastRemotingServer.registerProcessor(RequestCode.HEART_BEAT, clientManageProcessor, this.heartbeatExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.UNREGISTER_CLIENT, clientManageProcessor, this.clientManageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.CHECK_CLIENT_CONFIG, clientManageProcessor, this.clientManageExecutor);

        /**
         * ConsumerManageProcessor
         */
        ConsumerManageProcessor consumerManageProcessor = new ConsumerManageProcessor(this);
        this.remotingServer.registerProcessor(RequestCode.GET_CONSUMER_LIST_BY_GROUP, consumerManageProcessor, this.consumerManageExecutor);
        this.remotingServer.registerProcessor(RequestCode.UPDATE_CONSUMER_OFFSET, consumerManageProcessor, this.consumerManageExecutor);
        this.remotingServer.registerProcessor(RequestCode.QUERY_CONSUMER_OFFSET, consumerManageProcessor, this.consumerManageExecutor);

        this.fastRemotingServer.registerProcessor(RequestCode.GET_CONSUMER_LIST_BY_GROUP, consumerManageProcessor, this.consumerManageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.UPDATE_CONSUMER_OFFSET, consumerManageProcessor, this.consumerManageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.QUERY_CONSUMER_OFFSET, consumerManageProcessor, this.consumerManageExecutor);

        /**
         * QueryAssignmentProcessor
         */
        this.remotingServer.registerProcessor(RequestCode.QUERY_ASSIGNMENT, queryAssignmentProcessor, loadBalanceExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.QUERY_ASSIGNMENT, queryAssignmentProcessor, loadBalanceExecutor);
        this.remotingServer.registerProcessor(RequestCode.SET_MESSAGE_REQUEST_MODE, queryAssignmentProcessor, loadBalanceExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.SET_MESSAGE_REQUEST_MODE, queryAssignmentProcessor, loadBalanceExecutor);

        /**
         * EndTransactionProcessor
         */
        this.remotingServer.registerProcessor(RequestCode.END_TRANSACTION, endTransactionProcessor, this.endTransactionExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.END_TRANSACTION, endTransactionProcessor, this.endTransactionExecutor);

        /*
         * Default
         */
        AdminBrokerProcessor adminProcessor = new AdminBrokerProcessor(this);
        this.remotingServer.registerDefaultProcessor(adminProcessor, this.adminBrokerExecutor);
        this.fastRemotingServer.registerDefaultProcessor(adminProcessor, this.adminBrokerExecutor);
    }

    public BrokerStats getBrokerStats() {
        return brokerStats;
    }

    public void setBrokerStats(BrokerStats brokerStats) {
        this.brokerStats = brokerStats;
    }

    public void protectBroker() {
        if (this.brokerConfig.isDisableConsumeIfConsumerReadSlowly()) {
            for (Map.Entry<String, MomentStatsItem> next : this.brokerStatsManager.getMomentStatsItemSetFallSize().getStatsItemTable().entrySet()) {
                final long fallBehindBytes = next.getValue().getValue().get();
                if (fallBehindBytes > this.brokerConfig.getConsumerFallbehindThreshold()) {
                    final String[] split = next.getValue().getStatsKey().split("@");
                    final String group = split[2];
                    LOG_PROTECTION.info("[PROTECT_BROKER] the consumer[{}] consume slowly, {} bytes, disable it", group, fallBehindBytes);
                    this.subscriptionGroupManager.disableConsume(group);
                }
            }
        }
    }

    public long headSlowTimeMills(BlockingQueue<Runnable> q) {
        long slowTimeMills = 0;
        final Runnable peek = q.peek();
        if (peek != null) {
            RequestTask rt = BrokerFastFailure.castRunnable(peek);
            slowTimeMills = rt == null ? 0 : this.messageStore.now() - rt.getCreateTimestamp();
        }

        if (slowTimeMills < 0) {
            slowTimeMills = 0;
        }

        return slowTimeMills;
    }

    public long headSlowTimeMills4SendThreadPoolQueue() {
        return this.headSlowTimeMills(this.sendThreadPoolQueue);
    }

    public long headSlowTimeMills4PullThreadPoolQueue() {
        return this.headSlowTimeMills(this.pullThreadPoolQueue);
    }

    public long headSlowTimeMills4LitePullThreadPoolQueue() {
        return this.headSlowTimeMills(this.litePullThreadPoolQueue);
    }

    public long headSlowTimeMills4QueryThreadPoolQueue() {
        return this.headSlowTimeMills(this.queryThreadPoolQueue);
    }

    public void printWaterMark() {
        LOG_WATER_MARK.info("[WATERMARK] Send Queue Size: {} SlowTimeMills: {}", this.sendThreadPoolQueue.size(), headSlowTimeMills4SendThreadPoolQueue());
        LOG_WATER_MARK.info("[WATERMARK] Pull Queue Size: {} SlowTimeMills: {}", this.pullThreadPoolQueue.size(), headSlowTimeMills4PullThreadPoolQueue());
        LOG_WATER_MARK.info("[WATERMARK] Query Queue Size: {} SlowTimeMills: {}", this.queryThreadPoolQueue.size(), headSlowTimeMills4QueryThreadPoolQueue());
        LOG_WATER_MARK.info("[WATERMARK] Lite Pull Queue Size: {} SlowTimeMills: {}", this.litePullThreadPoolQueue.size(), headSlowTimeMills4LitePullThreadPoolQueue());
        LOG_WATER_MARK.info("[WATERMARK] Transaction Queue Size: {} SlowTimeMills: {}", this.endTransactionThreadPoolQueue.size(), headSlowTimeMills(this.endTransactionThreadPoolQueue));
        LOG_WATER_MARK.info("[WATERMARK] ClientManager Queue Size: {} SlowTimeMills: {}", this.clientManagerThreadPoolQueue.size(), this.headSlowTimeMills(this.clientManagerThreadPoolQueue));
        LOG_WATER_MARK.info("[WATERMARK] Heartbeat Queue Size: {} SlowTimeMills: {}", this.heartbeatThreadPoolQueue.size(), this.headSlowTimeMills(this.heartbeatThreadPoolQueue));
        LOG_WATER_MARK.info("[WATERMARK] Ack Queue Size: {} SlowTimeMills: {}", this.ackThreadPoolQueue.size(), headSlowTimeMills(this.ackThreadPoolQueue));
    }

    public MessageStore getMessageStore() {
        return messageStore;
    }

    public void setMessageStore(MessageStore messageStore) {
        this.messageStore = messageStore;
    }

    protected void printMasterAndSlaveDiff() {
        if (messageStore.getHaService() != null && messageStore.getHaService().getConnectionCount().get() > 0) {
            long diff = this.messageStore.slaveFallBehindMuch();
            LOG.info("CommitLog: slave fall behind master {}bytes", diff);
        }
    }

    public Broker2Client getBroker2Client() {
        return broker2Client;
    }

    public ConsumerManager getConsumerManager() {
        return consumerManager;
    }

    public ConsumerFilterManager getConsumerFilterManager() {
        return consumerFilterManager;
    }

    public ConsumerOrderInfoManager getConsumerOrderInfoManager() {
        return consumerOrderInfoManager;
    }

    public PopInflightMessageCounter getPopInflightMessageCounter() {
        return popInflightMessageCounter;
    }

    public ConsumerOffsetManager getConsumerOffsetManager() {
        return consumerOffsetManager;
    }

    public BroadcastOffsetManager getBroadcastOffsetManager() {
        return broadcastOffsetManager;
    }

    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }

    public ProducerManager getProducerManager() {
        return producerManager;
    }

    public void setFastRemotingServer(RemotingServer fastRemotingServer) {
        this.fastRemotingServer = fastRemotingServer;
    }

    public RemotingServer getFastRemotingServer() {
        return fastRemotingServer;
    }

    public PullMessageProcessor getPullMessageProcessor() {
        return pullMessageProcessor;
    }

    public PullRequestHoldService getPullRequestHoldService() {
        return pullRequestHoldService;
    }

    public void setSubscriptionGroupManager(SubscriptionGroupManager subscriptionGroupManager) {
        this.subscriptionGroupManager = subscriptionGroupManager;
    }

    public SubscriptionGroupManager getSubscriptionGroupManager() {
        return subscriptionGroupManager;
    }

    public PopMessageProcessor getPopMessageProcessor() {
        return popMessageProcessor;
    }

    public NotificationProcessor getNotificationProcessor() {
        return notificationProcessor;
    }

    public TimerMessageStore getTimerMessageStore() {
        return timerMessageStore;
    }

    public void setTimerMessageStore(TimerMessageStore timerMessageStore) {
        this.timerMessageStore = timerMessageStore;
    }

    public AckMessageProcessor getAckMessageProcessor() {
        return ackMessageProcessor;
    }

    public ChangeInvisibleTimeProcessor getChangeInvisibleTimeProcessor() {
        return changeInvisibleTimeProcessor;
    }

    protected void shutdownBasicService() {

        shutdown = true;

        this.unregisterBrokerAll();

        if (this.shutdownHook != null) {
            this.shutdownHook.beforeShutdown(this);
        }

        if (this.remotingServer != null) {
            this.remotingServer.shutdown();
        }

        if (this.fastRemotingServer != null) {
            this.fastRemotingServer.shutdown();
        }

        if (this.brokerStatsManager != null) {
            this.brokerStatsManager.shutdown();
        }

        if (this.clientHousekeepingService != null) {
            this.clientHousekeepingService.shutdown();
        }

        if (this.pullRequestHoldService != null) {
            this.pullRequestHoldService.shutdown();
        }

        {
            this.popMessageProcessor.getPopLongPollingService().shutdown();
            this.popMessageProcessor.getQueueLockManager().shutdown();
        }

        {
            this.popMessageProcessor.getPopBufferMergeService().shutdown();
            this.ackMessageProcessor.shutdownPopReviveService();
        }

        if (this.notificationProcessor != null) {
            this.notificationProcessor.shutdown();
        }

        if (this.consumerIdsChangeListener != null) {
            this.consumerIdsChangeListener.shutdown();
        }

        if (this.topicQueueMappingCleanService != null) {
            this.topicQueueMappingCleanService.shutdown();
        }
        //it is better to make sure the timerMessageStore shutdown firstly
        if (this.timerMessageStore != null) {
            this.timerMessageStore.shutdown();
        }
        if (this.fileWatchService != null) {
            this.fileWatchService.shutdown();
        }

        if (this.broadcastOffsetManager != null) {
            this.broadcastOffsetManager.shutdown();
        }

        if (this.messageStore != null) {
            this.messageStore.shutdown();
        }

        if (this.replicasManager != null) {
            this.replicasManager.shutdown();
        }

        shutdownScheduledExecutorService(this.scheduledExecutorService);

        if (this.sendMessageExecutor != null) {
            this.sendMessageExecutor.shutdown();
        }

        if (this.litePullMessageExecutor != null) {
            this.litePullMessageExecutor.shutdown();
        }

        if (this.pullMessageExecutor != null) {
            this.pullMessageExecutor.shutdown();
        }

        if (this.replyMessageExecutor != null) {
            this.replyMessageExecutor.shutdown();
        }

        if (this.putMessageFutureExecutor != null) {
            this.putMessageFutureExecutor.shutdown();
        }

        if (this.ackMessageExecutor != null) {
            this.ackMessageExecutor.shutdown();
        }

        if (this.adminBrokerExecutor != null) {
            this.adminBrokerExecutor.shutdown();
        }

        this.consumerOffsetManager.persist();

        if (this.filterServerManager != null) {
            this.filterServerManager.shutdown();
        }

        if (this.brokerFastFailure != null) {
            this.brokerFastFailure.shutdown();
        }

        if (this.consumerFilterManager != null) {
            this.consumerFilterManager.persist();
        }

        if (this.consumerOrderInfoManager != null) {
            this.consumerOrderInfoManager.persist();
        }

        if (this.scheduleMessageService != null) {
            this.scheduleMessageService.persist();
            this.scheduleMessageService.shutdown();
        }

        if (this.clientManageExecutor != null) {
            this.clientManageExecutor.shutdown();
        }

        if (this.queryMessageExecutor != null) {
            this.queryMessageExecutor.shutdown();
        }

        if (this.heartbeatExecutor != null) {
            this.heartbeatExecutor.shutdown();
        }

        if (this.consumerManageExecutor != null) {
            this.consumerManageExecutor.shutdown();
        }

        if (this.transactionalMessageCheckService != null) {
            this.transactionalMessageCheckService.shutdown(false);
        }

        if (this.endTransactionExecutor != null) {
            this.endTransactionExecutor.shutdown();
        }

        if (this.escapeBridge != null) {
            escapeBridge.shutdown();
        }

        if (this.topicRouteInfoManager != null) {
            this.topicRouteInfoManager.shutdown();
        }

        if (this.brokerPreOnlineService != null && !this.brokerPreOnlineService.isStopped()) {
            this.brokerPreOnlineService.shutdown();
        }

        shutdownScheduledExecutorService(this.syncBrokerMemberGroupExecutorService);
        shutdownScheduledExecutorService(this.brokerHeartbeatExecutorService);

        this.topicConfigManager.persist();
        this.subscriptionGroupManager.persist();

        for (BrokerAttachedPlugin brokerAttachedPlugin : brokerAttachedPlugins) {
            if (brokerAttachedPlugin != null) {
                brokerAttachedPlugin.shutdown();
            }
        }
    }

    public void shutdown() {

        shutdownBasicService();

        for (ScheduledFuture<?> scheduledFuture : scheduledFutures) {
            scheduledFuture.cancel(true);
        }

        if (this.brokerOuterAPI != null) {
            this.brokerOuterAPI.shutdown();
        }
    }

    protected void shutdownScheduledExecutorService(ScheduledExecutorService scheduledExecutorService) {
        if (scheduledExecutorService == null) {
            return;
        }
        scheduledExecutorService.shutdown();
        try {
            scheduledExecutorService.awaitTermination(5000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ignore) {
            BrokerController.LOG.warn("shutdown ScheduledExecutorService was Interrupted!  ", ignore);
            Thread.currentThread().interrupt();
        }
    }

    protected void unregisterBrokerAll() {
        this.brokerOuterAPI.unregisterBrokerAll(
            this.brokerConfig.getBrokerClusterName(),
            this.getBrokerAddr(),
            this.brokerConfig.getBrokerName(),
            this.brokerConfig.getBrokerId());
    }

    public String getBrokerAddr() {
        return this.brokerConfig.getBrokerIP1() + ":" + this.nettyServerConfig.getListenPort();
    }

    /**
     * 启动基础服务
     * @throws Exception
     */
    protected void startBasicService() throws Exception {
        //启动消息存储服务
        if (this.messageStore != null) {
            this.messageStore.start();
        }

        //启动延时消息存储服务
        if (this.timerMessageStore != null) {
            this.timerMessageStore.start();
        }

        //副本管理服务
        if (this.replicasManager != null) {
            this.replicasManager.start();
        }
        //启动netty远程服务
        if (remotingServerStartLatch != null) {
            remotingServerStartLatch.await();
        }
        //启动netty远程服务
        if (this.remotingServer != null) {
            this.remotingServer.start();

            // In test scenarios where it is up to OS to pick up an available port, set the listening port back to config
            if (null != nettyServerConfig && 0 == nettyServerConfig.getListenPort()) {
                nettyServerConfig.setListenPort(remotingServer.localListenPort());
            }
        }
        //启动快速netty远程服务
        if (this.fastRemotingServer != null) {
            this.fastRemotingServer.start();
        }

        this.storeHost = new InetSocketAddress(this.getBrokerConfig().getBrokerIP1(), this.getNettyServerConfig().getListenPort());

        for (BrokerAttachedPlugin brokerAttachedPlugin : brokerAttachedPlugins) {
            if (brokerAttachedPlugin != null) {
                brokerAttachedPlugin.start();
            }
        }

        if (this.popMessageProcessor != null) {
            this.popMessageProcessor.getPopLongPollingService().start();
            this.popMessageProcessor.getPopBufferMergeService().start();
            this.popMessageProcessor.getQueueLockManager().start();
        }

        //消息确认服务启动
        if (this.ackMessageProcessor != null) {
            this.ackMessageProcessor.startPopReviveService();
        }

        //消息队列清除启动
        if (this.topicQueueMappingCleanService != null) {
            this.topicQueueMappingCleanService.start();
        }
        //文件监听器启动
        if (this.fileWatchService != null) {
            this.fileWatchService.start();
        }
        //长轮询拉取消息挂起服务启动
        if (this.pullRequestHoldService != null) {
            this.pullRequestHoldService.start();
        }
        //客户端连接心跳服务启动
        if (this.clientHousekeepingService != null) {
            this.clientHousekeepingService.start();
        }

        if (this.filterServerManager != null) {
            this.filterServerManager.start();
        }

        //broker相关统计服务启动
        if (this.brokerStatsManager != null) {
            this.brokerStatsManager.start();
        }
        //broker快速失败服务启动
        if (this.brokerFastFailure != null) {
            this.brokerFastFailure.start();
        }

        if (this.broadcastOffsetManager != null) {
            this.broadcastOffsetManager.start();
        }

        if (this.escapeBridge != null) {
            this.escapeBridge.start();
        }

        if (this.topicRouteInfoManager != null) {
            this.topicRouteInfoManager.start();
        }

        if (this.brokerPreOnlineService != null) {
            this.brokerPreOnlineService.start();
        }

        //Init state version after messageStore initialized.
        this.topicConfigManager.initStateVersion();
    }
    /**
     * BrokerController的方法
     * 启动BrokerController
     */
    public void start() throws Exception {

        this.shouldStartTime = System.currentTimeMillis() + messageStoreConfig.getDisappearTimeAfterStart();

        if (messageStoreConfig.getTotalReplicas() > 1 && this.brokerConfig.isEnableSlaveActingMaster() || this.brokerConfig.isEnableControllerMode()) {
            isIsolated = true;
        }

        if (this.brokerOuterAPI != null) {
            this.brokerOuterAPI.start();
        }

        startBasicService();

        if (!isIsolated && !this.messageStoreConfig.isEnableDLegerCommitLog() && !this.messageStoreConfig.isDuplicationEnable()) {
            changeSpecialServiceStatus(this.brokerConfig.getBrokerId() == MixAll.MASTER_ID);
            /**
             * 强制注册broker到namesrv，避免集群中有多个broker时，只有一个broker注册成功，其他broker注册失败
             */
            this.registerBrokerAll(true, false, true);
        }

        //启动定时任务，默认30秒执行一次，时间间隔可以配置 brokerConfig.getRegisterNameServerPeriod() 允许 10000-60000ms之间
        scheduledFutures.add(this.scheduledExecutorService.scheduleAtFixedRate(new AbstractBrokerRunnable(this.getBrokerIdentity()) {
            @Override
            public void run0() {
                try {
                    if (System.currentTimeMillis() < shouldStartTime) {
                        BrokerController.LOG.info("Register to namesrv after {}", shouldStartTime);
                        return;
                    }
                    if (isIsolated) {
                        BrokerController.LOG.info("Skip register for broker is isolated");
                        return;
                    }
                    BrokerController.this.registerBrokerAll(true, false, brokerConfig.isForceRegister());
                } catch (Throwable e) {
                    BrokerController.LOG.error("registerBrokerAll Exception", e);
                }
            }
        }, 1000 * 10, Math.max(10000, Math.min(brokerConfig.getRegisterNameServerPeriod(), 60000)), TimeUnit.MILLISECONDS));

        if (this.brokerConfig.isEnableSlaveActingMaster()) {
            scheduleSendHeartbeat();

            scheduledFutures.add(this.syncBrokerMemberGroupExecutorService.scheduleAtFixedRate(new AbstractBrokerRunnable(this.getBrokerIdentity()) {
                @Override
                public void run0() {
                    try {
                        BrokerController.this.syncBrokerMemberGroup();
                    } catch (Throwable e) {
                        BrokerController.LOG.error("sync BrokerMemberGroup error. ", e);
                    }
                }
            }, 1000, this.brokerConfig.getSyncBrokerMemberGroupPeriod(), TimeUnit.MILLISECONDS));
        }

        if (this.brokerConfig.isEnableControllerMode()) {
            scheduleSendHeartbeat();
        }

        if (brokerConfig.isSkipPreOnline()) {
            startServiceWithoutCondition();
        }
    }

    protected void scheduleSendHeartbeat() {
        scheduledFutures.add(this.brokerHeartbeatExecutorService.scheduleAtFixedRate(new AbstractBrokerRunnable(this.getBrokerIdentity()) {
            @Override
            public void run0() {
                if (isIsolated) {
                    return;
                }
                try {
                    BrokerController.this.sendHeartbeat();
                } catch (Exception e) {
                    BrokerController.LOG.error("sendHeartbeat Exception", e);
                }

            }
        }, 1000, brokerConfig.getBrokerHeartbeatInterval(), TimeUnit.MILLISECONDS));
    }

    public synchronized void registerIncrementBrokerData(TopicConfig topicConfig, DataVersion dataVersion) {
        this.registerIncrementBrokerData(Collections.singletonList(topicConfig), dataVersion);
    }

    public synchronized void registerIncrementBrokerData(List<TopicConfig> topicConfigList, DataVersion dataVersion) {
        if (topicConfigList == null || topicConfigList.isEmpty()) {
            return;
        }

        TopicConfigAndMappingSerializeWrapper topicConfigSerializeWrapper = new TopicConfigAndMappingSerializeWrapper();
        topicConfigSerializeWrapper.setDataVersion(dataVersion);

        ConcurrentMap<String, TopicConfig> topicConfigTable = topicConfigList.stream()
            .map(topicConfig -> {
                TopicConfig registerTopicConfig;
                if (!PermName.isWriteable(this.getBrokerConfig().getBrokerPermission())
                    || !PermName.isReadable(this.getBrokerConfig().getBrokerPermission())) {
                    registerTopicConfig =
                        new TopicConfig(topicConfig.getTopicName(),
                            topicConfig.getReadQueueNums(),
                            topicConfig.getWriteQueueNums(),
                            this.brokerConfig.getBrokerPermission(), topicConfig.getTopicSysFlag());
                } else {
                    registerTopicConfig = new TopicConfig(topicConfig);
                }
                return registerTopicConfig;
            })
            .collect(Collectors.toConcurrentMap(TopicConfig::getTopicName, Function.identity()));
        topicConfigSerializeWrapper.setTopicConfigTable(topicConfigTable);

        Map<String, TopicQueueMappingInfo> topicQueueMappingInfoMap = topicConfigList.stream()
            .map(TopicConfig::getTopicName)
            .map(topicName -> Optional.ofNullable(this.topicQueueMappingManager.getTopicQueueMapping(topicName))
                .map(info -> new AbstractMap.SimpleImmutableEntry<>(topicName, TopicQueueMappingDetail.cloneAsMappingInfo(info)))
                .orElse(null))
            .filter(Objects::nonNull)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        if (!topicQueueMappingInfoMap.isEmpty()) {
            topicConfigSerializeWrapper.setTopicQueueMappingInfoMap(topicQueueMappingInfoMap);
        }

        doRegisterBrokerAll(true, false, topicConfigSerializeWrapper);
    }

    public synchronized void registerBrokerAll(final boolean checkOrderConfig, boolean oneway, boolean forceRegister) {

        //创建一个 TopicConfigAndMappingSerializeWrapper 对象 topicConfigWrapper，并设置其各项数据。
        //根据TopicConfigManager中的topic信息构建topic信息的传输协议对象，
        //在此前的topicConfigManager.load()方法中已经加载了所有topic信息，topic配置文件加载路径为{user.home}/store/config/topics.json
        TopicConfigAndMappingSerializeWrapper topicConfigWrapper = new TopicConfigAndMappingSerializeWrapper();

        //设置 topicConfigWrapper 的数据版本
        topicConfigWrapper.setDataVersion(this.getTopicConfigManager().getDataVersion());
        //设置 topicConfigWrapper 的 topicConfigTable
        topicConfigWrapper.setTopicConfigTable(this.getTopicConfigManager().getTopicConfigTable());

        //设置 topicConfigWrapper 的 topicQueueMappingInfoMap
        topicConfigWrapper.setTopicQueueMappingInfoMap(this.getTopicQueueMappingManager().getTopicQueueMappingTable().entrySet().stream().map(
            entry -> new AbstractMap.SimpleImmutableEntry<>(entry.getKey(), TopicQueueMappingDetail.cloneAsMappingInfo(entry.getValue()))
        ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));

        //进行一些权限检查，如果 Broker 的权限不可写或不可读，则创建一个新的 ConcurrentHashMap，遍历原有的 topicConfigTable，并根据 Broker 的权限重新生成 topicConfigTable。
        if (!PermName.isWriteable(this.getBrokerConfig().getBrokerPermission())
            || !PermName.isReadable(this.getBrokerConfig().getBrokerPermission())) {
            ConcurrentHashMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<>();
            for (TopicConfig topicConfig : topicConfigWrapper.getTopicConfigTable().values()) {
                //那么重新配置topic权限
                TopicConfig tmp =
                    new TopicConfig(topicConfig.getTopicName(), topicConfig.getReadQueueNums(), topicConfig.getWriteQueueNums(),
                        topicConfig.getPerm() & this.brokerConfig.getBrokerPermission(), topicConfig.getTopicSysFlag());
                topicConfigTable.put(topicConfig.getTopicName(), tmp);
            }
            topicConfigWrapper.setTopicConfigTable(topicConfigTable);
        }

        //如果满足注册条件（forceRegister 为 true，或需要注册），则调用 doRegisterBrokerAll 方法进行注册。
        //如果forceRegister为true，表示强制注册，或者如果当前broker应该注册，那么向nameServer进行注册
        if (forceRegister || needRegister(this.brokerConfig.getBrokerClusterName(),
            this.getBrokerAddr(),
            this.brokerConfig.getBrokerName(),
            this.brokerConfig.getBrokerId(),
            this.brokerConfig.getRegisterBrokerTimeoutMills(),
            this.brokerConfig.isInBrokerContainer())) {
            doRegisterBrokerAll(checkOrderConfig, oneway, topicConfigWrapper);
        }
    }

    /**
     * BrokerController的方法
     *
     * @param checkOrderConfig   是否检测顺序topic
     * @param oneway             是否是单向
     * @param topicConfigWrapper topic信息的传输协议包装对象
     */
    protected void doRegisterBrokerAll(boolean checkOrderConfig, boolean oneway,
        TopicConfigSerializeWrapper topicConfigWrapper) {

        if (shutdown) {
            BrokerController.LOG.info("BrokerController#doResterBrokerAll: broker has shutdown, no need to register any more.");
            return;
        }
        //执行注册，broker作为客户端向所有的nameserver发起注册请求
        List<RegisterBrokerResult> registerBrokerResultList = this.brokerOuterAPI.registerBrokerAll(
            this.brokerConfig.getBrokerClusterName(),
            this.getBrokerAddr(),
            this.brokerConfig.getBrokerName(),
            this.brokerConfig.getBrokerId(),
            this.getHAServerAddr(),
            //包含了携带topic信息的topicConfigTable，以及版本信息的dataVersion
            //这两个信息保存在持久化文件topics.json中
            topicConfigWrapper,
            this.filterServerManager.buildNewFilterServerList(),
            oneway,
            this.brokerConfig.getRegisterBrokerTimeoutMills(),
            this.brokerConfig.isEnableSlaveActingMaster(),
            this.brokerConfig.isCompressedRegister(),
            this.brokerConfig.isEnableSlaveActingMaster() ? this.brokerConfig.getBrokerNotActiveTimeoutMillis() : null,
            this.getBrokerIdentity());
        //对执行结果进行处理
        handleRegisterBrokerResult(registerBrokerResultList, checkOrderConfig);
    }

    protected void sendHeartbeat() {
        if (this.brokerConfig.isEnableControllerMode()) {
            final List<String> controllerAddresses = this.replicasManager.getAvailableControllerAddresses();
            for (String controllerAddress : controllerAddresses) {
                if (StringUtils.isNotEmpty(controllerAddress)) {
                    this.brokerOuterAPI.sendHeartbeatToController(
                        controllerAddress,
                        this.brokerConfig.getBrokerClusterName(),
                        this.getBrokerAddr(),
                        this.brokerConfig.getBrokerName(),
                        this.brokerConfig.getBrokerId(),
                        this.brokerConfig.getSendHeartbeatTimeoutMillis(),
                        this.brokerConfig.isInBrokerContainer(), this.replicasManager.getLastEpoch(),
                        this.messageStore.getMaxPhyOffset(),
                        this.replicasManager.getConfirmOffset(),
                        this.brokerConfig.getControllerHeartBeatTimeoutMills(),
                        this.brokerConfig.getBrokerElectionPriority()
                    );
                }
            }
        }

        if (this.brokerConfig.isEnableSlaveActingMaster()) {
            if (this.brokerConfig.isCompatibleWithOldNameSrv()) {
                this.brokerOuterAPI.sendHeartbeatViaDataVersion(
                    this.brokerConfig.getBrokerClusterName(),
                    this.getBrokerAddr(),
                    this.brokerConfig.getBrokerName(),
                    this.brokerConfig.getBrokerId(),
                    this.brokerConfig.getSendHeartbeatTimeoutMillis(),
                    this.getTopicConfigManager().getDataVersion(),
                    this.brokerConfig.isInBrokerContainer());
            } else {
                this.brokerOuterAPI.sendHeartbeat(
                    this.brokerConfig.getBrokerClusterName(),
                    this.getBrokerAddr(),
                    this.brokerConfig.getBrokerName(),
                    this.brokerConfig.getBrokerId(),
                    this.brokerConfig.getSendHeartbeatTimeoutMillis(),
                    this.brokerConfig.isInBrokerContainer());
            }
        }
    }

    protected void syncBrokerMemberGroup() {
        try {
            brokerMemberGroup = this.getBrokerOuterAPI()
                .syncBrokerMemberGroup(this.brokerConfig.getBrokerClusterName(), this.brokerConfig.getBrokerName(), this.brokerConfig.isCompatibleWithOldNameSrv());
        } catch (Exception e) {
            BrokerController.LOG.error("syncBrokerMemberGroup from namesrv failed, ", e);
            return;
        }
        if (brokerMemberGroup == null || brokerMemberGroup.getBrokerAddrs().size() == 0) {
            BrokerController.LOG.warn("Couldn't find any broker member from namesrv in {}/{}", this.brokerConfig.getBrokerClusterName(), this.brokerConfig.getBrokerName());
            return;
        }
        this.messageStore.setAliveReplicaNumInGroup(calcAliveBrokerNumInGroup(brokerMemberGroup.getBrokerAddrs()));

        if (!this.isIsolated) {
            long minBrokerId = brokerMemberGroup.minimumBrokerId();
            this.updateMinBroker(minBrokerId, brokerMemberGroup.getBrokerAddrs().get(minBrokerId));
        }
    }

    private int calcAliveBrokerNumInGroup(Map<Long, String> brokerAddrTable) {
        if (brokerAddrTable.containsKey(this.brokerConfig.getBrokerId())) {
            return brokerAddrTable.size();
        } else {
            return brokerAddrTable.size() + 1;
        }
    }

    protected void handleRegisterBrokerResult(List<RegisterBrokerResult> registerBrokerResultList,
        boolean checkOrderConfig) {
        for (RegisterBrokerResult registerBrokerResult : registerBrokerResultList) {
            if (registerBrokerResult != null) {
                if (this.updateMasterHAServerAddrPeriodically && registerBrokerResult.getHaServerAddr() != null) {
                    this.messageStore.updateHaMasterAddress(registerBrokerResult.getHaServerAddr());
                    this.messageStore.updateMasterAddress(registerBrokerResult.getMasterAddr());
                }

                this.slaveSynchronize.setMasterAddr(registerBrokerResult.getMasterAddr());
                if (checkOrderConfig) {
                    this.getTopicConfigManager().updateOrderTopicConfig(registerBrokerResult.getKvTable());
                }
                break;
            }
        }
    }


    /**
     * BrokerController的方法
     * <p>
     * broker是否需要向nemeserver中注册
     *
     * @param clusterName  集群名
     * @param brokerAddr   broker地址
     * @param brokerName   broker名字
     * @param brokerId     brkerId
     * @param timeoutMills 超时时间
     * @return broker是否需要向nameserver中注册
     */
    private boolean needRegister(final String clusterName,
        final String brokerAddr,
        final String brokerName,
        final long brokerId,
        final int timeoutMills,
        final boolean isInBrokerContainer) {

        //根据TopicConfigManager中的topic信息构建topic信息的传输协议对象，
        //在此前的topicConfigManager.load()方法中已经加载了所有topic信息，topic配置文件加载路径为{user.home}/store/config/topics.json
        TopicConfigSerializeWrapper topicConfigWrapper = this.getTopicConfigManager().buildTopicConfigSerializeWrapper();
        //获取所有nameServer的DataVersion数据，一一对比自身数据是否一致，如果有一个nameserver的DataVersion数据版本不一致则重新注册
        List<Boolean> changeList = brokerOuterAPI.needRegister(clusterName, brokerAddr, brokerName, brokerId, topicConfigWrapper, timeoutMills, isInBrokerContainer);
        boolean needRegister = false;
        //如果和一个nameServer的数据版本不一致，则需要重新注册
        for (Boolean changed : changeList) {
            if (changed) {
                needRegister = true;
                break;
            }
        }
        return needRegister;
    }

    public void startService(long minBrokerId, String minBrokerAddr) {
        BrokerController.LOG.info("{} start service, min broker id is {}, min broker addr: {}",
            this.brokerConfig.getCanonicalName(), minBrokerId, minBrokerAddr);
        this.minBrokerIdInGroup = minBrokerId;
        this.minBrokerAddrInGroup = minBrokerAddr;

        this.changeSpecialServiceStatus(this.brokerConfig.getBrokerId() == minBrokerId);
        this.registerBrokerAll(true, false, brokerConfig.isForceRegister());

        isIsolated = false;
    }

    public void startServiceWithoutCondition() {
        BrokerController.LOG.info("{} start service", this.brokerConfig.getCanonicalName());

        this.changeSpecialServiceStatus(this.brokerConfig.getBrokerId() == MixAll.MASTER_ID);
        this.registerBrokerAll(true, false, brokerConfig.isForceRegister());

        isIsolated = false;
    }

    public void stopService() {
        BrokerController.LOG.info("{} stop service", this.getBrokerConfig().getCanonicalName());
        isIsolated = true;
        this.changeSpecialServiceStatus(false);
    }

    public boolean isSpecialServiceRunning() {
        if (isScheduleServiceStart() && isTransactionCheckServiceStart()) {
            return true;
        }

        return this.ackMessageProcessor != null && this.ackMessageProcessor.isPopReviveServiceRunning();
    }

    private void onMasterOffline() {
        // close channels with master broker
        String masterAddr = this.slaveSynchronize.getMasterAddr();
        if (masterAddr != null) {
            this.brokerOuterAPI.getRemotingClient().closeChannels(
                Arrays.asList(masterAddr, MixAll.brokerVIPChannel(true, masterAddr)));
        }
        // master not available, stop sync
        this.slaveSynchronize.setMasterAddr(null);
        this.messageStore.updateHaMasterAddress(null);
    }

    private void onMasterOnline(String masterAddr, String masterHaAddr) {
        boolean needSyncMasterFlushOffset = this.messageStore.getMasterFlushedOffset() == 0
            && this.messageStoreConfig.isSyncMasterFlushOffsetWhenStartup();
        if (masterHaAddr == null || needSyncMasterFlushOffset) {
            try {
                BrokerSyncInfo brokerSyncInfo = this.brokerOuterAPI.retrieveBrokerHaInfo(masterAddr);

                if (needSyncMasterFlushOffset) {
                    LOG.info("Set master flush offset in slave to {}", brokerSyncInfo.getMasterFlushOffset());
                    this.messageStore.setMasterFlushedOffset(brokerSyncInfo.getMasterFlushOffset());
                }

                if (masterHaAddr == null) {
                    this.messageStore.updateHaMasterAddress(brokerSyncInfo.getMasterHaAddress());
                    this.messageStore.updateMasterAddress(brokerSyncInfo.getMasterAddress());
                }
            } catch (Exception e) {
                LOG.error("retrieve master ha info exception, {}", e);
            }
        }

        // set master HA address.
        if (masterHaAddr != null) {
            this.messageStore.updateHaMasterAddress(masterHaAddr);
        }

        // wakeup HAClient
        this.messageStore.wakeupHAClient();
    }

    private void onMinBrokerChange(long minBrokerId, String minBrokerAddr, String offlineBrokerAddr,
        String masterHaAddr) {
        LOG.info("Min broker changed, old: {}-{}, new {}-{}",
            this.minBrokerIdInGroup, this.minBrokerAddrInGroup, minBrokerId, minBrokerAddr);

        this.minBrokerIdInGroup = minBrokerId;
        this.minBrokerAddrInGroup = minBrokerAddr;

        this.changeSpecialServiceStatus(this.brokerConfig.getBrokerId() == this.minBrokerIdInGroup);

        if (offlineBrokerAddr != null && offlineBrokerAddr.equals(this.slaveSynchronize.getMasterAddr())) {
            // master offline
            onMasterOffline();
        }

        if (minBrokerId == MixAll.MASTER_ID && minBrokerAddr != null) {
            // master online
            onMasterOnline(minBrokerAddr, masterHaAddr);
        }

        // notify PullRequest on hold to pull from master.
        if (this.minBrokerIdInGroup == MixAll.MASTER_ID) {
            this.pullRequestHoldService.notifyMasterOnline();
        }
    }

    public void updateMinBroker(long minBrokerId, String minBrokerAddr) {
        if (brokerConfig.isEnableSlaveActingMaster() && brokerConfig.getBrokerId() != MixAll.MASTER_ID) {
            if (lock.tryLock()) {
                try {
                    if (minBrokerId != this.minBrokerIdInGroup) {
                        String offlineBrokerAddr = null;
                        if (minBrokerId > this.minBrokerIdInGroup) {
                            offlineBrokerAddr = this.minBrokerAddrInGroup;
                        }
                        onMinBrokerChange(minBrokerId, minBrokerAddr, offlineBrokerAddr, null);
                    }
                } finally {
                    lock.unlock();
                }
            }
        }
    }

    public void updateMinBroker(long minBrokerId, String minBrokerAddr, String offlineBrokerAddr,
        String masterHaAddr) {
        if (brokerConfig.isEnableSlaveActingMaster() && brokerConfig.getBrokerId() != MixAll.MASTER_ID) {
            try {
                if (lock.tryLock(3000, TimeUnit.MILLISECONDS)) {
                    try {
                        if (minBrokerId != this.minBrokerIdInGroup) {
                            onMinBrokerChange(minBrokerId, minBrokerAddr, offlineBrokerAddr, masterHaAddr);
                        }
                    } finally {
                        lock.unlock();
                    }

                }
            } catch (InterruptedException e) {
                LOG.error("Update min broker error, {}", e);
            }
        }
    }

    /**
     * 用于控制特殊服务的状态
     * @param shouldStart
     */
    public void changeSpecialServiceStatus(boolean shouldStart) {

        //对 brokerAttachedPlugins 集合中的每个 BrokerAttachedPlugin 调用 statusChanged(shouldStart) 方法。这些插件可能会根据 shouldStart 参数来改变它们的状态
        for (BrokerAttachedPlugin brokerAttachedPlugin : brokerAttachedPlugins) {
            if (brokerAttachedPlugin != null) {
                brokerAttachedPlugin.statusChanged(shouldStart);
            }
        }

        //对定时任务服务进行状态切换
        changeScheduleServiceStatus(shouldStart);

        //对事务服务进行状态切换
        changeTransactionCheckServiceStatus(shouldStart);

        //检查 ackMessageProcessor 对象是否为非空，如果是，会设置 PopReviveService 的状态为 shouldStart，并记录相应的信息。
        if (this.ackMessageProcessor != null) {
            LOG.info("Set PopReviveService Status to {}", shouldStart);
            this.ackMessageProcessor.setPopReviveServiceStatus(shouldStart);
        }
    }

    private synchronized void changeTransactionCheckServiceStatus(boolean shouldStart) {
        if (isTransactionCheckServiceStart != shouldStart) {
            LOG.info("TransactionCheckService status changed to {}", shouldStart);
            if (shouldStart) {
                this.transactionalMessageCheckService.start();
            } else {
                this.transactionalMessageCheckService.shutdown(true);
            }
            isTransactionCheckServiceStart = shouldStart;
        }
    }

    public synchronized void changeScheduleServiceStatus(boolean shouldStart) {
        if (isScheduleServiceStart != shouldStart) {
            LOG.info("ScheduleServiceStatus changed to {}", shouldStart);
            if (shouldStart) {
                this.scheduleMessageService.start();
            } else {
                this.scheduleMessageService.stop();
            }
            isScheduleServiceStart = shouldStart;

            if (timerMessageStore != null) {
                timerMessageStore.setShouldRunningDequeue(shouldStart);
            }
        }
    }

    public MessageStore getMessageStoreByBrokerName(String brokerName) {
        if (this.brokerConfig.getBrokerName().equals(brokerName)) {
            return this.getMessageStore();
        }
        return null;
    }

    public BrokerIdentity getBrokerIdentity() {
        if (messageStoreConfig.isEnableDLegerCommitLog()) {
            return new BrokerIdentity(
                brokerConfig.getBrokerClusterName(), brokerConfig.getBrokerName(),
                Integer.parseInt(messageStoreConfig.getdLegerSelfId().substring(1)), brokerConfig.isInBrokerContainer());
        } else {
            return new BrokerIdentity(
                brokerConfig.getBrokerClusterName(), brokerConfig.getBrokerName(),
                brokerConfig.getBrokerId(), brokerConfig.isInBrokerContainer());
        }
    }

    public TopicConfigManager getTopicConfigManager() {
        return topicConfigManager;
    }

    public void setTopicConfigManager(TopicConfigManager topicConfigManager) {
        this.topicConfigManager = topicConfigManager;
    }

    public TopicQueueMappingManager getTopicQueueMappingManager() {
        return topicQueueMappingManager;
    }

    public String getHAServerAddr() {
        return this.brokerConfig.getBrokerIP2() + ":" + this.messageStoreConfig.getHaListenPort();
    }

    public RebalanceLockManager getRebalanceLockManager() {
        return rebalanceLockManager;
    }

    public SlaveSynchronize getSlaveSynchronize() {
        return slaveSynchronize;
    }

    public ScheduledExecutorService getScheduledExecutorService() {
        return scheduledExecutorService;
    }

    public ExecutorService getPullMessageExecutor() {
        return pullMessageExecutor;
    }

    public ExecutorService getPutMessageFutureExecutor() {
        return putMessageFutureExecutor;
    }

    public void setPullMessageExecutor(ExecutorService pullMessageExecutor) {
        this.pullMessageExecutor = pullMessageExecutor;
    }

    public BlockingQueue<Runnable> getSendThreadPoolQueue() {
        return sendThreadPoolQueue;
    }

    public BlockingQueue<Runnable> getAckThreadPoolQueue() {
        return ackThreadPoolQueue;
    }

    public FilterServerManager getFilterServerManager() {
        return filterServerManager;
    }

    public BrokerStatsManager getBrokerStatsManager() {
        return brokerStatsManager;
    }

    public List<SendMessageHook> getSendMessageHookList() {
        return sendMessageHookList;
    }

    public void registerSendMessageHook(final SendMessageHook hook) {
        this.sendMessageHookList.add(hook);
        LOG.info("register SendMessageHook Hook, {}", hook.hookName());
    }

    public List<ConsumeMessageHook> getConsumeMessageHookList() {
        return consumeMessageHookList;
    }

    public void registerConsumeMessageHook(final ConsumeMessageHook hook) {
        this.consumeMessageHookList.add(hook);
        LOG.info("register ConsumeMessageHook Hook, {}", hook.hookName());
    }

    public void registerServerRPCHook(RPCHook rpcHook) {
        getRemotingServer().registerRPCHook(rpcHook);
        this.fastRemotingServer.registerRPCHook(rpcHook);
    }

    public RemotingServer getRemotingServer() {
        return remotingServer;
    }

    public void setRemotingServer(RemotingServer remotingServer) {
        this.remotingServer = remotingServer;
    }

    public CountDownLatch getRemotingServerStartLatch() {
        return remotingServerStartLatch;
    }

    public void setRemotingServerStartLatch(CountDownLatch remotingServerStartLatch) {
        this.remotingServerStartLatch = remotingServerStartLatch;
    }

    public void registerClientRPCHook(RPCHook rpcHook) {
        this.getBrokerOuterAPI().registerRPCHook(rpcHook);
    }

    public BrokerOuterAPI getBrokerOuterAPI() {
        return brokerOuterAPI;
    }

    public InetSocketAddress getStoreHost() {
        return storeHost;
    }

    public void setStoreHost(InetSocketAddress storeHost) {
        this.storeHost = storeHost;
    }

    public Configuration getConfiguration() {
        return this.configuration;
    }

    public BlockingQueue<Runnable> getHeartbeatThreadPoolQueue() {
        return heartbeatThreadPoolQueue;
    }

    public TransactionalMessageCheckService getTransactionalMessageCheckService() {
        return transactionalMessageCheckService;
    }

    public void setTransactionalMessageCheckService(
        TransactionalMessageCheckService transactionalMessageCheckService) {
        this.transactionalMessageCheckService = transactionalMessageCheckService;
    }

    public TransactionalMessageService getTransactionalMessageService() {
        return transactionalMessageService;
    }

    public void setTransactionalMessageService(TransactionalMessageService transactionalMessageService) {
        this.transactionalMessageService = transactionalMessageService;
    }

    public AbstractTransactionalMessageCheckListener getTransactionalMessageCheckListener() {
        return transactionalMessageCheckListener;
    }

    public void setTransactionalMessageCheckListener(
        AbstractTransactionalMessageCheckListener transactionalMessageCheckListener) {
        this.transactionalMessageCheckListener = transactionalMessageCheckListener;
    }

    public BlockingQueue<Runnable> getEndTransactionThreadPoolQueue() {
        return endTransactionThreadPoolQueue;

    }

    public Map<Class, AccessValidator> getAccessValidatorMap() {
        return accessValidatorMap;
    }

    public ExecutorService getSendMessageExecutor() {
        return sendMessageExecutor;
    }

    public SendMessageProcessor getSendMessageProcessor() {
        return sendMessageProcessor;
    }

    public QueryAssignmentProcessor getQueryAssignmentProcessor() {
        return queryAssignmentProcessor;
    }

    public TopicQueueMappingCleanService getTopicQueueMappingCleanService() {
        return topicQueueMappingCleanService;
    }

    public ExecutorService getAdminBrokerExecutor() {
        return adminBrokerExecutor;
    }

    public BlockingQueue<Runnable> getLitePullThreadPoolQueue() {
        return litePullThreadPoolQueue;
    }

    public ShutdownHook getShutdownHook() {
        return shutdownHook;
    }

    public void setShutdownHook(ShutdownHook shutdownHook) {
        this.shutdownHook = shutdownHook;
    }

    public long getMinBrokerIdInGroup() {
        return this.brokerConfig.getBrokerId();
    }

    public BrokerController peekMasterBroker() {
        return brokerConfig.getBrokerId() == MixAll.MASTER_ID ? this : null;
    }

    public BrokerMemberGroup getBrokerMemberGroup() {
        return this.brokerMemberGroup;
    }

    public int getListenPort() {
        return this.nettyServerConfig.getListenPort();
    }

    public List<BrokerAttachedPlugin> getBrokerAttachedPlugins() {
        return brokerAttachedPlugins;
    }

    public EscapeBridge getEscapeBridge() {
        return escapeBridge;
    }

    public long getShouldStartTime() {
        return shouldStartTime;
    }

    public BrokerPreOnlineService getBrokerPreOnlineService() {
        return brokerPreOnlineService;
    }

    public EndTransactionProcessor getEndTransactionProcessor() {
        return endTransactionProcessor;
    }

    public boolean isScheduleServiceStart() {
        return isScheduleServiceStart;
    }

    public boolean isTransactionCheckServiceStart() {
        return isTransactionCheckServiceStart;
    }

    public ScheduleMessageService getScheduleMessageService() {
        return scheduleMessageService;
    }

    public ReplicasManager getReplicasManager() {
        return replicasManager;
    }

    public void setIsolated(boolean isolated) {
        isIsolated = isolated;
    }

    public boolean isIsolated() {
        return this.isIsolated;
    }

    public TimerCheckpoint getTimerCheckpoint() {
        return timerCheckpoint;
    }

    public TopicRouteInfoManager getTopicRouteInfoManager() {
        return this.topicRouteInfoManager;
    }

    public BlockingQueue<Runnable> getClientManagerThreadPoolQueue() {
        return clientManagerThreadPoolQueue;
    }

    public BlockingQueue<Runnable> getConsumerManagerThreadPoolQueue() {
        return consumerManagerThreadPoolQueue;
    }

    public BlockingQueue<Runnable> getAsyncPutThreadPoolQueue() {
        return putThreadPoolQueue;
    }

    public BlockingQueue<Runnable> getReplyThreadPoolQueue() {
        return replyThreadPoolQueue;
    }

    public BlockingQueue<Runnable> getAdminBrokerThreadPoolQueue() {
        return adminBrokerThreadPoolQueue;
    }

}
