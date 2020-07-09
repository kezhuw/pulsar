/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.client.api;

import static org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest.retryStrategically;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.netty.buffer.ByteBuf;

import java.lang.reflect.Method;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import lombok.extern.slf4j.Slf4j;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.meta.AbstractZkLedgerManager;
import org.apache.bookkeeper.meta.CleanupLedgerManager;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieRequestProcessor;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.versioning.Version;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.impl.SimpleLoadManagerImpl;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.BrokerStats;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.powermock.reflect.Whitebox;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.asserts.SoftAssert;

@Slf4j
public class ClientDeduplicationFailureTest {
    LocalBookkeeperEnsemble bkEnsemble;

    ServiceConfiguration config;
    URL url;
    PulsarService pulsar;
    PulsarAdmin admin;
    PulsarClient pulsarClient;
    BrokerStats brokerStatsClient;
    final String tenant = "external-repl-prop";
    String primaryHost;

    @BeforeMethod(timeOut = 300000)
    void setup(Method method) throws Exception {
        log.info("--- Setting up method {} ---", method.getName());

        // Start local bookkeeper ensemble
        bkEnsemble = new LocalBookkeeperEnsemble(3, 0, () -> 0);
        bkEnsemble.start();

        config = spy(new ServiceConfiguration());
        config.setClusterName("use");
        config.setWebServicePort(Optional.of(0));
        config.setZookeeperServers("127.0.0.1" + ":" + bkEnsemble.getZookeeperPort());
        config.setBrokerServicePort(Optional.of(0));
        config.setLoadManagerClassName(SimpleLoadManagerImpl.class.getName());
        config.setTlsAllowInsecureConnection(true);
        config.setAdvertisedAddress("localhost");
        config.setLoadBalancerSheddingEnabled(false);
        config.setLoadBalancerAutoBundleSplitEnabled(false);
        config.setLoadBalancerEnabled(false);
        config.setLoadBalancerAutoUnloadSplitBundlesEnabled(false);

        config.setAllowAutoTopicCreationType("non-partitioned");


        pulsar = new PulsarService(config);
        pulsar.start();

        String brokerServiceUrl = pulsar.getWebServiceAddress();
        url = new URL(brokerServiceUrl);

        admin = PulsarAdmin.builder().serviceHttpUrl(brokerServiceUrl).build();

        brokerStatsClient = admin.brokerStats();
        primaryHost = pulsar.getWebServiceAddress();

        // update cluster metadata
        ClusterData clusterData = new ClusterData(url.toString());
        admin.clusters().createCluster(config.getClusterName(), clusterData);

        ClientBuilder clientBuilder = PulsarClient.builder().serviceUrl(pulsar.getBrokerServiceUrl()).maxBackoffInterval(1, TimeUnit.SECONDS);
        pulsarClient = clientBuilder.build();

        TenantInfo tenantInfo = new TenantInfo();
        tenantInfo.setAllowedClusters(Sets.newHashSet(Lists.newArrayList("use")));
        admin.tenants().createTenant(tenant, tenantInfo);
    }

    @AfterMethod
    void shutdown() throws Exception {
        log.info("--- Shutting down ---");
        pulsarClient.close();
        admin.close();
        pulsar.close();
        bkEnsemble.stop();
    }

    private static class ProducerThread implements Runnable {

        private volatile boolean isRunning = false;
        private Thread thread;
        private Producer<String> producer;
        private long i = 1;
        private AtomicLong atomicLong = new AtomicLong(0);
        private CompletableFuture<MessageId> lastMessageFuture;

        public ProducerThread(Producer<String> producer) {
            this.thread = new Thread(this);
            this.producer = producer;
        }

        @Override
        public void run() {
            while(isRunning) {
                lastMessageFuture = producer.newMessage().sequenceId(i).value("foo-" + i).sendAsync();
                lastMessageFuture.thenAccept(messageId -> {
                    atomicLong.incrementAndGet();

                }).exceptionally(ex -> {
                    log.info("publish exception:", ex);
                    return null;
                });
                i++;
            }
            log.info("done Producing! Last send: {}", i);
        }

        public void start() {
            this.isRunning = true;
            this.thread.start();
        }

        public void stop() {
            this.isRunning = false;
            try {
                log.info("Waiting for last message to complete");
                try {
                    this.lastMessageFuture.get(60, TimeUnit.SECONDS);
                } catch (TimeoutException e) {
                    throw new RuntimeException("Last message hasn't completed within timeout!");
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            log.info("Producer Thread stopped!");
        }

        public long getLastSeqId() {
            return this.atomicLong.get();
        }
    }

    @Test(timeOut = 300000)
    public void testClientDeduplicationCorrectnessWithFailure() throws Exception {
        final String namespacePortion = "dedup";
        final String replNamespace = tenant + "/" + namespacePortion;
        final String sourceTopic = "persistent://" + replNamespace + "/my-topic1";
        admin.namespaces().createNamespace(replNamespace);
        Set<String> clusters = Sets.newHashSet(Lists.newArrayList("use"));
        admin.namespaces().setNamespaceReplicationClusters(replNamespace, clusters);
        admin.namespaces().setDeduplicationStatus(replNamespace, true);
        admin.namespaces().setRetention(replNamespace, new RetentionPolicies(-1, -1));
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .blockIfQueueFull(true).sendTimeout(0, TimeUnit.SECONDS)
                .topic(sourceTopic)
                .producerName("test-producer-1")
                .create();


        ProducerThread producerThread = new ProducerThread(producer);
        producerThread.start();

        retryStrategically((test) -> {
            try {
                TopicStats topicStats = admin.topics().getStats(sourceTopic);
                return topicStats.publishers.size() == 1 && topicStats.publishers.get(0).getProducerName().equals("test-producer-1") && topicStats.storageSize > 0;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 5, 200);

        TopicStats topicStats = admin.topics().getStats(sourceTopic);
        assertEquals(topicStats.publishers.size(), 1);
        assertEquals(topicStats.publishers.get(0).getProducerName(), "test-producer-1");
        assertTrue(topicStats.storageSize > 0);

        for (int i = 0; i < 5; i++) {
            log.info("Stopping BK...");
            bkEnsemble.stopBK();

            Thread.sleep(1000 + new Random().nextInt(500));

            log.info("Starting BK...");
            bkEnsemble.startBK();
        }

        producerThread.stop();

        // send last message
        producer.newMessage().sequenceId(producerThread.getLastSeqId() + 1).value("end").send();
        producer.close();

        Reader<String> reader = pulsarClient.newReader(Schema.STRING).startMessageId(MessageId.earliest).topic(sourceTopic).create();
        Message<String> prevMessage = null;
        Message<String> message = null;
        int count = 0;
        while(true) {
            message = reader.readNext(5, TimeUnit.SECONDS);
            if (message == null) {
                break;
            }

            if (message.getValue().equals("end")) {
                log.info("Last seq Id received: {}", prevMessage.getSequenceId());
                break;
            }
            if (prevMessage == null) {
                assertEquals(message.getSequenceId(), 1);
            } else {
                assertEquals(message.getSequenceId(), prevMessage.getSequenceId() + 1);
            }
            prevMessage = message;
            count++;
        }

        log.info("# of messages read: {}", count);

        assertTrue(prevMessage != null);
        assertEquals(prevMessage.getSequenceId(), producerThread.getLastSeqId());
    }

    @Test(timeOut = 300000)
    public void testClientDeduplicationWithBkFailure() throws  Exception {
        final String namespacePortion = "dedup";
        final String replNamespace = tenant + "/" + namespacePortion;
        final String sourceTopic = "persistent://" + replNamespace + "/my-topic1";
        final String subscriptionName1 = "sub1";
        final String subscriptionName2 = "sub2";
        final String consumerName1 = "test-consumer-1";
        final String consumerName2 = "test-consumer-2";
        final List<Message<String>> msgRecvd = new LinkedList<>();
        admin.namespaces().createNamespace(replNamespace);
        Set<String> clusters = Sets.newHashSet(Lists.newArrayList("use"));
        admin.namespaces().setNamespaceReplicationClusters(replNamespace, clusters);
        admin.namespaces().setDeduplicationStatus(replNamespace, true);
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(sourceTopic)
                .producerName("test-producer-1").create();
        Consumer<String> consumer1 = pulsarClient.newConsumer(Schema.STRING).topic(sourceTopic)
                .consumerName(consumerName1).subscriptionName(subscriptionName1).subscribe();
        Consumer<String> consumer2 = pulsarClient.newConsumer(Schema.STRING).topic(sourceTopic)
                .consumerName(consumerName2).subscriptionName(subscriptionName2).subscribe();

        Thread thread = new Thread(() -> {
            while(true) {
                try {
                    Message<String> msg = consumer2.receive();
                    msgRecvd.add(msg);
                    consumer2.acknowledge(msg);
                } catch (PulsarClientException e) {
                    log.error("Failed to consume message: {}", e, e);
                    break;
                }
            }
        });
        thread.start();

        retryStrategically((test) -> {
            try {
                TopicStats topicStats = admin.topics().getStats(sourceTopic);
                boolean c1 =  topicStats!= null
                        && topicStats.subscriptions.get(subscriptionName1) != null
                        && topicStats.subscriptions.get(subscriptionName1).consumers.size() == 1
                        && topicStats.subscriptions.get(subscriptionName1).consumers.get(0).consumerName.equals(consumerName1);

                boolean c2 =  topicStats!= null
                        && topicStats.subscriptions.get(subscriptionName2) != null
                        && topicStats.subscriptions.get(subscriptionName2).consumers.size() == 1
                        && topicStats.subscriptions.get(subscriptionName2).consumers.get(0).consumerName.equals(consumerName2);
                return c1 && c2;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 5, 200);

        TopicStats topicStats1 = admin.topics().getStats(sourceTopic);
        assertTrue(topicStats1!= null);
        assertTrue(topicStats1.subscriptions.get(subscriptionName1) != null);
        assertEquals(topicStats1.subscriptions.get(subscriptionName1).consumers.size(), 1);
        assertEquals(topicStats1.subscriptions.get(subscriptionName1).consumers.get(0).consumerName, consumerName1);
        TopicStats topicStats2 = admin.topics().getStats(sourceTopic);
        assertTrue(topicStats2!= null);
        assertTrue(topicStats2.subscriptions.get(subscriptionName2) != null);
        assertEquals(topicStats2.subscriptions.get(subscriptionName2).consumers.size(), 1);
        assertEquals(topicStats2.subscriptions.get(subscriptionName2).consumers.get(0).consumerName, consumerName2);

        for (int i=0; i<10; i++) {
            producer.newMessage().sequenceId(i).value("foo-" + i).send();
        }

        for (int i=0; i<10; i++) {
            Message<String> msg = consumer1.receive();
            consumer1.acknowledge(msg);
            assertEquals(msg.getValue(), "foo-" + i);
            assertEquals(msg.getSequenceId(), i);
        }

        log.info("Stopping BK...");
        bkEnsemble.stopBK();

        List<CompletableFuture<MessageId>> futures = new LinkedList<>();
        for (int i=10; i<20; i++) {
            CompletableFuture<MessageId> future = producer.newMessage().sequenceId(i).value("foo-" + i).sendAsync();
            int finalI = i;
            future.thenRun(() -> log.error("message: {} successful", finalI)).exceptionally((Function<Throwable, Void>) throwable -> {
                log.info("message: {} failed: {}", finalI, throwable, throwable);
                return null;
            });
            futures.add(future);
        }

        for (int i = 0; i < futures.size(); i++) {
            try {
                // message should not be produced successfully
                futures.get(i).join();
                fail();
            } catch (CompletionException ex) {

            } catch (Exception e) {
                fail();
            }
        }

        try {
            producer.newMessage().sequenceId(10).value("foo-10").send();
            fail();
        } catch (PulsarClientException ex) {

        }

        try {
            producer.newMessage().sequenceId(10).value("foo-10").send();
            fail();
        } catch (PulsarClientException ex) {

        }

        log.info("Starting BK...");
        bkEnsemble.startBK();

        for (int i=20; i<30; i++) {
            producer.newMessage().sequenceId(i).value("foo-" + i).send();
        }

        MessageId lastMessageId = null;
        for (int i=20; i<30; i++) {
            Message<String> msg = consumer1.receive();
            lastMessageId = msg.getMessageId();
            consumer1.acknowledge(msg);
            assertEquals(msg.getValue(), "foo-" + i);
            assertEquals(msg.getSequenceId(), i);
        }

        // check all messages
        retryStrategically((test) -> msgRecvd.size() >= 20, 5, 200);

        assertEquals(msgRecvd.size(), 20);
        for (int i=0; i<10; i++) {
            assertEquals(msgRecvd.get(i).getValue(), "foo-" + i);
            assertEquals(msgRecvd.get(i).getSequenceId(), i);
        }
        for (int i=10; i<20; i++) {
            assertEquals(msgRecvd.get(i).getValue(), "foo-" + (i + 10));
            assertEquals(msgRecvd.get(i).getSequenceId(), i + 10);
        }

        BatchMessageIdImpl batchMessageId = (BatchMessageIdImpl) lastMessageId;
        MessageIdImpl messageId = (MessageIdImpl) consumer1.getLastMessageId();

        assertEquals(messageId.getLedgerId(), batchMessageId.getLedgerId());
        assertEquals(messageId.getEntryId(), batchMessageId.getEntryId());
        thread.interrupt();
    }

    @Test
    public void testClientDeduplicationWithBkFailureAndZkDisconnected() throws Exception {
        final String namespacePortion = "dedup";
        final String replNamespace = tenant + "/" + namespacePortion;
        final String sourceTopic = "persistent://" + replNamespace + "/my-topic1";
        final String subscriptionName1 = "sub1";
        final String consumerName1 = "test-consumer-1";
        admin.namespaces().createNamespace(replNamespace);
        Set<String> clusters = Sets.newHashSet(Lists.newArrayList("use"));
        admin.namespaces().setNamespaceReplicationClusters(replNamespace, clusters);
        admin.namespaces().setDeduplicationStatus(replNamespace, true);

        // Stop one bookie, so we known that the remaining two bookies form the ensemble,
        // write quorum and ack quorum.
        bkEnsemble.stopBK(2);

        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(sourceTopic)
                .producerName("test-producer-1").create();

        Consumer<String> consumer1 = pulsarClient.newConsumer(Schema.STRING)
                .topic(sourceTopic)
                .consumerName(consumerName1)
                .subscriptionName(subscriptionName1)
                .subscribe();

        for (int i = 1; i <= 10; i++) {
            producer.newMessage().sequenceId(i).value("foo-" + i).send();
        }

        BookieServer[] bookieServers = bkEnsemble.getBookies();
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopic(sourceTopic, false).join().get();
        ManagedLedgerImpl managedLedger = (ManagedLedgerImpl) topic.getManagedLedger();
        BookKeeper bookKeeper = Whitebox.getInternalState(managedLedger, "bookKeeper");
        CleanupLedgerManager cleanupLedgerManager = (CleanupLedgerManager) bookKeeper.getLedgerManager();
        AbstractZkLedgerManager zkLedgerManager = (AbstractZkLedgerManager) cleanupLedgerManager.getUnderlying();
        ZooKeeper zooKeeper = Whitebox.getInternalState(zkLedgerManager, "zk");

        // Disconnect zookeeper client whiling closing ledger.
        //
        // This will make ledger remains in open state in zookeeper.
        AbstractZkLedgerManager spyZkLedgerManager = spy(zkLedgerManager);
        Whitebox.setInternalState(cleanupLedgerManager, "underlying", spyZkLedgerManager);
        doAnswer(invocation -> {
            bkEnsemble.disconnectZookeeper(zooKeeper);
            // Reset to original ledger manager.
            Whitebox.setInternalState(cleanupLedgerManager, "underlying", zkLedgerManager);
            return invocation.callRealMethod();
        })
            .when(spyZkLedgerManager)
            .writeLedgerMetadata(any(long.class), any(LedgerMetadata.class), any(Version.class));

        // Future that will be fulfilled after reconnect.
        CompletableFuture<Void> reconnectedFuture = new CompletableFuture<>();
        zooKeeper.exists("/", (WatchedEvent event) -> {
            Watcher.Event.KeeperState state = event.getState();
            if (state == Watcher.Event.KeeperState.SyncConnected) {
                reconnectedFuture.complete(null);
            }
        });

        BookieServer bookieServer1 = bookieServers[1];
        Bookie bookie1 = bookieServer1.getBookie();
        BookieRequestProcessor requestProcessor1 = bookieServer1.getBookieRequestProcessor();

        // Spy bookie to shutdown after write request persisted to mimic bookie crash.
        //
        // There will be no enough ensemble for ledger to function which cause write
        // failure and ledger closing.
        Bookie spyBookie1 = spy(bookie1);
        Whitebox.setInternalState(requestProcessor1, "bookie", spyBookie1);
        doAnswer(invocation -> {
            ByteBuf entry = invocation.getArgument(0);
            byte[] masterKey = invocation.getArgument(4);
            BookkeeperInternalCallbacks.WriteCallback spyCb = (int rc, long ledgerId, long entryId, BookieSocketAddress addr, Object ctx) -> {
                ForkJoinPool.commonPool().execute(() -> {
                    bkEnsemble.stopBK(1);
                });
            };
            bookie1.addEntry(entry, false, spyCb, null, masterKey);
            return null;
        })
            .when(spyBookie1)
            .addEntry(
                any(ByteBuf.class),
                any(boolean.class),
                any(BookkeeperInternalCallbacks.WriteCallback.class),
                any(Object.class),
                any(byte[].class)
            );

        // Spy bookkeeper to start stopped bookie to form ensemble and wait
        // zookeeper reconnected to create new ledger.
        BookKeeper spyBookKeeper = spy(bookKeeper);
        Whitebox.setInternalState(managedLedger, "bookKeeper", spyBookKeeper);
        doAnswer(invocation -> {
            bkEnsemble.startBK(1);
            reconnectedFuture.join();
            Whitebox.setInternalState(managedLedger, "bookKeeper", bookKeeper);
            return invocation.callRealMethod();
        })
            .when(spyBookKeeper)
            .asyncCreateLedger(
                any(int.class),
                any(int.class),
                any(int.class),
                any(BookKeeper.DigestType.class),
                any(byte[].class),
                any(AsyncCallback.CreateCallback.class),
                any(Object.class),
                any(Map.class)
            );

        try {
            // Due to spies above, this sending will fail in current ledger
            // and retry then succeed in new ledger.
            producer.newMessage().sequenceId(11).value("foo-11").send();
        } catch (Exception ex) {
            fail(ex.getMessage(), ex);
        }

        List<MLDataFormats.ManagedLedgerInfo.LedgerInfo> ledgerInfos = managedLedger.getLedgersInfoAsList();
        assertEquals(ledgerInfos.size(), 2);

        MLDataFormats.ManagedLedgerInfo.LedgerInfo ledgerInfo = ledgerInfos.get(0);
        assertEquals(ledgerInfo.getEntries(), 10);

        SoftAssert assertion = new SoftAssert();

        // Assert that whether pulsar `LedgerInfo` and bookkeeper `LedgerMetadata` are consistent,
        long ledgerId = ledgerInfo.getLedgerId();
        ManagedLedgerConfig config = managedLedger.getConfig();
        LedgerHandle handle = bookKeeper.openLedger(ledgerId, BookKeeper.DigestType.fromApiDigestType(config.getDigestType()), config.getPassword());
        assertion.assertEquals(handle.getLedgerMetadata().getLastEntryId(), 9, "unexpected last entry id");
        assertion.assertEquals(handle.getLastAddConfirmed(), 9, "unexpected last add confirmed");

        // Add new entries so that we can safely read pass above added entries.
        final int n = 13;
        for (int i = 12; i <= n; i++) {
            producer.newMessage().sequenceId(i).value("foo-" + i).send();
        }

        for (int i = 1; i <= n; i++) {
            Message<String> msg = consumer1.receive();
            consumer1.acknowledge(msg);
            assertion.assertEquals(msg.getSequenceId(), i, "unexpected sequence id");
            assertion.assertEquals(msg.getValue(), "foo-" + i, "unexpected message content");
        }

        assertion.assertAll();
    }
}
