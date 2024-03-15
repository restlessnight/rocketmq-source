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

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;

public class BrokerStartup {

    public static Logger log;
    public static final SystemConfigFileHelper CONFIG_FILE_HELPER = new SystemConfigFileHelper();

    public static void main(String[] args) {
        //启动broker的入口
        //创建并启动一个BrokerController实例
        //1.Broker的启动同样可以分为两个大步骤，一个是实例化和初始化BrokerController，
        //另一个启动BrokerController，这一点和nameserver模块的启动来说是一样的。
        //2.Broker模块启动时还会加载很多的文件，这个文件包括topic的各种信息topics.json，
        //消费者的各种信息consumerOffset.json等等文件，重要的是还会加载各种消息文件，例如commitlog、consumequeue、indexfile文件等等，在加载了消息文件之后，会对文件进行恢复。
        start(createBrokerController(args));
    }

    /**
     * BrokerStartup的方法
     *
     * @param controller 被启动的BrokerController
     */
    public static BrokerController start(BrokerController controller) {
        try {
            //启动broker
            controller.start();

            //broker启动之后的的信息打印到控制台
            String tip = String.format("The broker[%s, %s] boot success. serializeType=%s",
                controller.getBrokerConfig().getBrokerName(), controller.getBrokerAddr(),
                RemotingCommand.getSerializeTypeConfigInThisServer());

            if (null != controller.getBrokerConfig().getNamesrvAddr()) {
                tip += " and name server is " + controller.getBrokerConfig().getNamesrvAddr();
            }

            log.info(tip);
            System.out.printf("%s%n", tip);
            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    public static void shutdown(final BrokerController controller) {
        if (null != controller) {
            controller.shutdown();
        }
    }

    /**
     * 构建一个brokerController
     * 解析命令行，加载Broker配置，以及NettyServer、NettyClient的各种配置（解析命令行中-c指定的配置文件）并保存起来，
     * 然后进行一些配置的校验，日志的配置，随后创建一个BrokerController实例。
     * @param args
     * @return
     * @throws Exception
     */
    public static BrokerController buildBrokerController(String[] args) throws Exception {
        //设置RocketMQ的版本信息，设置属性rocketmq.remoting.version，即当前rocketmq版本
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));

        /*
         * 1.创建broker各种配置类的配置类
         */
        //创建Broker的配置类，包含Broker的各种配置，比如ROCKETMQ_HOME
        final BrokerConfig brokerConfig = new BrokerConfig();
        //NettyServer的配置类，Broker作为服务端，比如接收来自客户端的消息的时候
        final NettyServerConfig nettyServerConfig = new NettyServerConfig();
        //NettyClient的配置类，Broker还会作为客户端，比如连接NameServer的时候
        final NettyClientConfig nettyClientConfig = new NettyClientConfig();
        //Broker的消息存储配置，例如各种文件大小等
        final MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        //设置作为NettyServer时的监听端口为10911
        nettyServerConfig.setListenPort(10911);
        //PackageConflictDetect.detectFastjson();
        /*
         * jar包启动时，构建命令行操作的指令，使用main方法启动可以忽略
         */
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        //mq broker命令文件
        CommandLine commandLine = ServerUtil.parseCmdLine(
            "mqbroker", args, buildCommandlineOptions(options), new DefaultParser());
        if (null == commandLine) {
            System.exit(-1);
        }

        /*
         * 解析外部配置文件
         * 判断命令行中是否包含字符'c'，即是否包含通过命令行指定配置文件的命令
         * 例如，启动Broker的时候添加的 -c /Volumes/Samsung/Idea/rocketmq/config/conf/broker.conf命令
         */
        Properties properties = null;
        if (commandLine.hasOption('c')) {
            //获取该命令指定的配置文件
            String file = commandLine.getOptionValue('c');
            if (file != null) {
                //加载外部配置文件
                CONFIG_FILE_HELPER.setFile(file);
                //设置配置文件路径
                BrokerPathConfigHelper.setBrokerConfigPath(file);
                properties = CONFIG_FILE_HELPER.loadConfig();
            }
        }

        if (properties != null) {
            //将rmqAddressServerDomain、rmqAddressServerSubGroup属性设置为系统属性
            properties2SystemEnv(properties);
            //设置broker的配置信息
            MixAll.properties2Object(properties, brokerConfig);
            //设置nettyServer的配置信息
            MixAll.properties2Object(properties, nettyServerConfig);
            //设置nettyClient的配置信息
            MixAll.properties2Object(properties, nettyClientConfig);
            //设置messageStore的配置信息
            MixAll.properties2Object(properties, messageStoreConfig);
        }
        //设置broker的配置信息
        MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), brokerConfig);
        //如果不存在ROCKETMQ_HOME的配置，那么打印异常并退出程序
        if (null == brokerConfig.getRocketmqHome()) {
            System.out.printf("Please set the %s variable in your environment " +
                "to match the location of the RocketMQ installation", MixAll.ROCKETMQ_HOME_ENV);
            System.exit(-2);
        }

        /*
         * 获取namesrvAddr，即NameServer的地址，并进行校验
         */
        // Validate namesrvAddr
        String namesrvAddr = brokerConfig.getNamesrvAddr();
        if (StringUtils.isNotBlank(namesrvAddr)) {
            try {
                //拆分NameServer的地址
                //可以指定多个NameServer的地址，以";"分隔
                String[] addrArray = namesrvAddr.split(";");
                for (String addr : addrArray) {
                    //将字符串的地址，转换为网络连接的SocketAddress，检测格式是否正确
                    NetworkUtil.string2SocketAddress(addr);
                }
            } catch (Exception e) {
                System.out.printf("The Name Server Address[%s] illegal, please set it as follows, " +
                        "\"127.0.0.1:9876;192.168.0.1:9876\"%n", namesrvAddr);
                System.exit(-3);
            }
        }

        //如果broker的角色是slave，设置命中消息在内存的最大比例
        //默认broker角色是异步master
        if (BrokerRole.SLAVE == messageStoreConfig.getBrokerRole()) {
            //30
            int ratio = messageStoreConfig.getAccessMessageInMemoryMaxRatio() - 10;
            messageStoreConfig.setAccessMessageInMemoryMaxRatio(ratio);
        }

        // Set broker role according to ha config
        if (!brokerConfig.isEnableControllerMode()) {
            /*
             * 设置、校验brokerId
             *
             * 根据broker的角色配置brokerId，默认角色是ASYNC_MASTER
             * 通过此配置可知BrokerId为0表示Master，非0表示Slave
             */
            switch (messageStoreConfig.getBrokerRole()) {
                case ASYNC_MASTER:
                case SYNC_MASTER:
                    brokerConfig.setBrokerId(MixAll.MASTER_ID);
                    break;
                case SLAVE:
                    if (brokerConfig.getBrokerId() <= MixAll.MASTER_ID) {
                        System.out.printf("Slave's brokerId must be > 0%n");
                        System.exit(-3);
                    }
                    break;
                default:
                    break;
            }
        }
        // 开启 DLeger 的操作
        if (messageStoreConfig.isEnableDLegerCommitLog()) {
            brokerConfig.setBrokerId(-1);
        }
        /*
         * 5 设置高可用通信监听端口，为监听端口+1，默认就是10912
         * 该端口主要用于比如主从同步之类的高可用操作
         *
         * 在配置broker集群的时候需要注意，配置集群时可能会抛出：Address already in use
         * 因为一个broker机器会占用三个端口，监听ip端口，以及监听ip端口+1的端口，监听ip端口-2端口
         */
        messageStoreConfig.setHaListenPort(nettyServerConfig.getListenPort() + 1);
        brokerConfig.setInBrokerContainer(false);

        /*
         * 日志相关配置
         */
        System.setProperty("brokerLogDir", "");
        if (brokerConfig.isIsolateLogEnable()) {
            System.setProperty("brokerLogDir", brokerConfig.getBrokerName() + "_" + brokerConfig.getBrokerId());
        }
        if (brokerConfig.isIsolateLogEnable() && messageStoreConfig.isEnableDLegerCommitLog()) {
            System.setProperty("brokerLogDir", brokerConfig.getBrokerName() + "_" + messageStoreConfig.getdLegerSelfId());
        }
        /*判断命令行中是否包含字符'p'（printConfigItem）和'm'，如果存在则打印配置信息并结束jvm运行，没有的话就不用管*/
        if (commandLine.hasOption('p')) {
            Logger console = LoggerFactory.getLogger(LoggerName.BROKER_CONSOLE_NAME);
            MixAll.printObjectProperties(console, brokerConfig);
            MixAll.printObjectProperties(console, nettyServerConfig);
            MixAll.printObjectProperties(console, nettyClientConfig);
            MixAll.printObjectProperties(console, messageStoreConfig);
            System.exit(0);
        } else if (commandLine.hasOption('m')) {
            Logger console = LoggerFactory.getLogger(LoggerName.BROKER_CONSOLE_NAME);
            MixAll.printObjectProperties(console, brokerConfig, true);
            MixAll.printObjectProperties(console, nettyServerConfig, true);
            MixAll.printObjectProperties(console, nettyClientConfig, true);
            MixAll.printObjectProperties(console, messageStoreConfig, true);
            System.exit(0);
        }
        //打印当前broker的配置日志
        log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
        MixAll.printObjectProperties(log, brokerConfig);
        MixAll.printObjectProperties(log, nettyServerConfig);
        MixAll.printObjectProperties(log, nettyClientConfig);
        MixAll.printObjectProperties(log, messageStoreConfig);
        /*
         * 实例化BrokerController，设置各种属性
         */
        final BrokerController controller = new BrokerController(
            brokerConfig, nettyServerConfig, nettyClientConfig, messageStoreConfig);

        // Remember all configs to prevent discard
        // 将所有的-c的外部配置信息保存到NamesrvController中的Configuration对象属性的allConfigs属性中
        controller.getConfiguration().registerConfig(properties);

        return controller;
    }

    /**
     * 添加关闭钩子方法，在Broker关闭之前执行，进行一些内存清理、对象销毁等操作
     * @param brokerController
     * @return
     */
    public static Runnable buildShutdownHook(BrokerController brokerController) {
        return new Runnable() {
            private volatile boolean hasShutdown = false;
            private final AtomicInteger shutdownTimes = new AtomicInteger(0);

            @Override
            public void run() {
                synchronized (this) {
                    log.info("Shutdown hook was invoked, {}", this.shutdownTimes.incrementAndGet());
                    if (!this.hasShutdown) {
                        this.hasShutdown = true;
                        long beginTime = System.currentTimeMillis();
                        //执行controller的shutdown方法，并且还会在messageStore#shutdown方法中将abort临时文件删除。
                        brokerController.shutdown();
                        long consumingTimeTotal = System.currentTimeMillis() - beginTime;
                        log.info("Shutdown hook over, consuming total time(ms): {}", consumingTimeTotal);
                    }
                }
            }
        };
    }

    /**
     * BrokerController相当于Broker的一个中央控制器类。创建了BrokerController实例之后，再调用initialize方法进行初始化操作。这是核心方法。
     * @param args
     * @return
     */
    public static BrokerController createBrokerController(String[] args) {
        try {
            BrokerController controller = buildBrokerController(args);
            /*
             * 初始化BrokerController
             * 创建netty远程服务，初始化Netty线程池，注册请求处理器，配置定时任务，用于扫描并移除不活跃的Broker等操作。
             */
            boolean initResult = controller.initialize();
            //初始化失败则退出
            if (!initResult) {
                controller.shutdown();
                System.exit(-3);
            }
            /*
             * 添加关闭钩子方法，在Broker关闭之前执行，进行一些内存清理、对象销毁等操作
             */
            Runtime.getRuntime().addShutdownHook(new Thread(buildShutdownHook(controller)));
            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return null;
    }

    private static void properties2SystemEnv(Properties properties) {
        if (properties == null) {
            return;
        }
        String rmqAddressServerDomain = properties.getProperty("rmqAddressServerDomain", MixAll.WS_DOMAIN_NAME);
        String rmqAddressServerSubGroup = properties.getProperty("rmqAddressServerSubGroup", MixAll.WS_DOMAIN_SUBGROUP);
        System.setProperty("rocketmq.namesrv.domain", rmqAddressServerDomain);
        System.setProperty("rocketmq.namesrv.domain.subgroup", rmqAddressServerSubGroup);
    }

    private static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("c", "configFile", true, "Broker config properties file");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "printConfigItem", false, "Print all config item");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("m", "printImportantConfig", false, "Print important config item");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    public static class SystemConfigFileHelper {
        private static final Logger LOGGER = LoggerFactory.getLogger(SystemConfigFileHelper.class);

        private String file;

        public SystemConfigFileHelper() {
        }

        public Properties loadConfig() throws Exception {
            InputStream in = new BufferedInputStream(Files.newInputStream(Paths.get(file)));
            Properties properties = new Properties();
            properties.load(in);
            in.close();
            return properties;
        }

        public void update(Properties properties) throws Exception {
            LOGGER.error("[SystemConfigFileHelper] update no thing.");
        }

        public void setFile(String file) {
            this.file = file;
        }

        public String getFile() {
            return file;
        }
    }
}
