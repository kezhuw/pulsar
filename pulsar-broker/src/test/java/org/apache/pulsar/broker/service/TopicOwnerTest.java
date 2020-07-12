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
package org.apache.pulsar.broker.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.powermock.api.mockito.PowerMockito.doAnswer;
import static org.powermock.api.mockito.PowerMockito.spy;

import com.google.common.collect.Sets;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.jute.Record;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.lookup.LookupResult;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.namespace.ServiceUnitZkUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.proto.DeleteRequest;
import org.apache.zookeeper.proto.ReplyHeader;
import org.apache.zookeeper.server.ByteBufferInputStream;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.mockito.stubbing.Answer;
import org.powermock.reflect.Whitebox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Optional;

public class TopicOwnerTest {

    private static final Logger log = LoggerFactory.getLogger(TopicOwnerTest.class);

    LocalBookkeeperEnsemble bkEnsemble;
    protected PulsarAdmin[] pulsarAdmins = new PulsarAdmin[BROKER_COUNT];
    protected static final int BROKER_COUNT = 5;
    protected ServiceConfiguration[] configurations = new ServiceConfiguration[BROKER_COUNT];
    protected PulsarService[] pulsarServices = new PulsarService[BROKER_COUNT];
    protected PulsarService leaderPulsar;
    protected PulsarAdmin leaderAdmin;

    @BeforeMethod
    void setup() throws Exception {
        log.info("---- Initializing TopicOwnerTest -----");
        // Start local bookkeeper ensemble
        bkEnsemble = new LocalBookkeeperEnsemble(3, 0, () -> 0);
        bkEnsemble.start();

        // start brokers
        for (int i = 0; i < BROKER_COUNT; i++) {
            ServiceConfiguration config = new ServiceConfiguration();
            config.setBrokerServicePort(Optional.of(0));
            config.setClusterName("my-cluster");
            config.setAdvertisedAddress("localhost");
            config.setWebServicePort(Optional.of(0));
            config.setZookeeperServers("127.0.0.1" + ":" + bkEnsemble.getZookeeperPort());
            config.setDefaultNumberOfNamespaceBundles(1);
            config.setLoadBalancerEnabled(false);
            configurations[i] = config;

            pulsarServices[i] = new PulsarService(config);
            pulsarServices[i].start();

            // Sleep until pulsarServices[0] becomes leader, this way we can spy namespace bundle assignment easily.
            while (i == 0 && !pulsarServices[0].getLeaderElectionService().isLeader()) {
                Thread.sleep(10);
            }

            pulsarAdmins[i] = PulsarAdmin.builder()
                    .serviceHttpUrl(pulsarServices[i].getWebServiceAddress())
                    .build();
        }
        leaderPulsar = pulsarServices[0];
        leaderAdmin = pulsarAdmins[0];
        Thread.sleep(1000);
    }

    @Test
    public void testUnloadingWithZookeeperDisconnected() throws Exception {
        pulsarAdmins[0].clusters().createCluster("my-cluster", new ClusterData(pulsarServices[0].getWebServiceAddress()));
        TenantInfo tenantInfo = new TenantInfo();
        tenantInfo.setAllowedClusters(Sets.newHashSet("my-cluster"));
        pulsarAdmins[0].tenants().createTenant("my-tenant", tenantInfo);
        pulsarAdmins[0].namespaces().createNamespace("my-tenant/my-ns", 16);

        TopicName topicName = TopicName.get("persistent://my-tenant/my-ns/topic-1");
        NamespaceService leaderNamespaceService = leaderPulsar.getNamespaceService();
        NamespaceBundle namespaceBundle = leaderNamespaceService.getBundle(topicName);

        // Spy leader namespace service to inject authorized broker for namespace-bundle from leader,
        // this is a safe operation since it is just an recommendation if namespace-bundle has no owner
        // currently. Namespace-bundle ownership contention is an atomic operation through zookeeper.
        NamespaceService spyLeaderNamespaceService = spy(leaderNamespaceService);
        final MutableObject<PulsarService> leaderAuthorizedBroker = new MutableObject<>();
        Answer<CompletableFuture<Optional<LookupResult>>> answer = invocation -> {
            PulsarService pulsarService = leaderAuthorizedBroker.getValue();
            if (pulsarService == null) {
                return CompletableFuture.completedFuture(Optional.empty());
            }
            LookupResult lookupResult = new LookupResult(
                    pulsarService.getWebServiceAddress(),
                    pulsarService.getWebServiceAddressTls(),
                    pulsarService.getBrokerServiceUrl(),
                    pulsarService.getBrokerServiceUrlTls(),
                    true);
            return CompletableFuture.completedFuture(Optional.of(lookupResult));
        };
        doAnswer(answer).when(spyLeaderNamespaceService).getBrokerServiceUrlAsync(any(TopicName.class), any(boolean.class), nullable(String.class));
        Whitebox.setInternalState(leaderPulsar, "nsService", spyLeaderNamespaceService);

        PulsarService pulsar1 = pulsarServices[1];
        PulsarService pulsar2 = pulsarServices[2];

        leaderAuthorizedBroker.setValue(pulsar1);
        Assert.assertEquals(pulsarAdmins[0].lookups().lookupTopic(topicName.toString()), pulsar1.getBrokerServiceUrl());
        Assert.assertEquals(pulsarAdmins[1].lookups().lookupTopic(topicName.toString()), pulsar1.getBrokerServiceUrl());
        Assert.assertEquals(pulsarAdmins[2].lookups().lookupTopic(topicName.toString()), pulsar1.getBrokerServiceUrl());
        Assert.assertEquals(pulsarAdmins[3].lookups().lookupTopic(topicName.toString()), pulsar1.getBrokerServiceUrl());
        Assert.assertEquals(pulsarAdmins[4].lookups().lookupTopic(topicName.toString()), pulsar1.getBrokerServiceUrl());

        leaderAuthorizedBroker.setValue(pulsar2);
        Assert.assertEquals(pulsarAdmins[0].lookups().lookupTopic(topicName.toString()), pulsar1.getBrokerServiceUrl());
        Assert.assertEquals(pulsarAdmins[1].lookups().lookupTopic(topicName.toString()), pulsar1.getBrokerServiceUrl());
        Assert.assertEquals(pulsarAdmins[2].lookups().lookupTopic(topicName.toString()), pulsar1.getBrokerServiceUrl());
        Assert.assertEquals(pulsarAdmins[3].lookups().lookupTopic(topicName.toString()), pulsar1.getBrokerServiceUrl());
        Assert.assertEquals(pulsarAdmins[4].lookups().lookupTopic(topicName.toString()), pulsar1.getBrokerServiceUrl());

        ZooKeeper zooKeeper1 = pulsar1.getZkClient();

        // Future that will be fulfilled after reconnect.
        CompletableFuture<Void> reconnectedFuture = new CompletableFuture<>();
        zooKeeper1.exists("/", (WatchedEvent event) -> {
            Watcher.Event.KeeperState state = event.getState();
            if (state == Watcher.Event.KeeperState.SyncConnected) {
                reconnectedFuture.complete(null);
            }
        });

        ZooKeeperServer zooKeeperServer = bkEnsemble.getZkServer();
        ServerCnxn zkServerConnection1 = bkEnsemble.getZookeeperServerConnection(zooKeeper1);
        ZooKeeperServer spyZooKeeperServer = spy(zooKeeperServer);
        String namespaceBundlePath = ServiceUnitZkUtils.path(namespaceBundle);

        // Spy zk server connection to close connection to cause connection loss after namespace-bundle
        // deleted successfully. This mimics crash of connected zk server after committing requested operation.
        Whitebox.setInternalState(zkServerConnection1, "zkServer", spyZooKeeperServer);
        doAnswer(invocation -> {
            Request request = invocation.getArgument(0);
            if (request.sessionId != zooKeeper1.getSessionId() || request.type != ZooDefs.OpCode.delete) {
                return invocation.callRealMethod();
            }
            DeleteRequest deleteRequest = new DeleteRequest();
            ByteBufferInputStream.byteBuffer2Record(request.request.duplicate(), deleteRequest);
            if (!deleteRequest.getPath().contains(namespaceBundlePath)) {
                return invocation.callRealMethod();
            }
            Whitebox.setInternalState(zkServerConnection1, "zkServer", zooKeeperServer);

            ServerCnxn spyZkServerConnection1 = spy(zkServerConnection1);
            MutableBoolean disconnected = new MutableBoolean();
            doAnswer(responseInvocation -> {
                synchronized (disconnected) {
                    ReplyHeader replyHeader = responseInvocation.getArgument(0);
                    if (replyHeader.getXid() == request.cxid) {
                        disconnected.setTrue();
                        bkEnsemble.disconnectZookeeper(zooKeeper1);
                    } else if (disconnected.isFalse()) {
                        return responseInvocation.callRealMethod();
                    }
                    return null;
                }
            }).when(spyZkServerConnection1).sendResponse(any(ReplyHeader.class), nullable(Record.class), any(String.class));
            Whitebox.setInternalState(request, "cnxn", spyZkServerConnection1);
            return invocation.callRealMethod();
        }).when(spyZooKeeperServer).submitRequest(any(Request.class));

        try {
            pulsarAdmins[1].namespaces().unloadNamespaceBundle(namespaceBundle.getNamespaceObject().toString(), namespaceBundle.getBundleRange());
        } catch (Exception ex) {
            // Ignored since whether failing unloading when zk connection-loss is an implementation detail.
        }

        reconnectedFuture.join();

        Assert.assertEquals(pulsarAdmins[0].lookups().lookupTopic(topicName.toString()), pulsar2.getBrokerServiceUrl());
        Assert.assertEquals(pulsarAdmins[2].lookups().lookupTopic(topicName.toString()), pulsar2.getBrokerServiceUrl());
        Assert.assertEquals(pulsarAdmins[3].lookups().lookupTopic(topicName.toString()), pulsar2.getBrokerServiceUrl());
        Assert.assertEquals(pulsarAdmins[4].lookups().lookupTopic(topicName.toString()), pulsar2.getBrokerServiceUrl());

        Assert.assertEquals(pulsarAdmins[1].lookups().lookupTopic(topicName.toString()), pulsar2.getBrokerServiceUrl());
    }

    @Test
    public void testConnectToInvalidateBundleCacheBroker() throws Exception {
        pulsarAdmins[0].clusters().createCluster("my-cluster", new ClusterData(pulsarServices[0].getWebServiceAddress()));
        TenantInfo tenantInfo = new TenantInfo();
        tenantInfo.setAllowedClusters(Sets.newHashSet("my-cluster"));
        pulsarAdmins[0].tenants().createTenant("my-tenant", tenantInfo);
        pulsarAdmins[0].namespaces().createNamespace("my-tenant/my-ns", 16);

        Assert.assertEquals(pulsarAdmins[0].namespaces().getPolicies("my-tenant/my-ns").bundles.getNumBundles(), 16);

        final String topic1 = "persistent://my-tenant/my-ns/topic-1";
        final String topic2 = "persistent://my-tenant/my-ns/topic-2";

        // Do topic lookup here for broker to own namespace bundles
        String serviceUrlForTopic1 = pulsarAdmins[0].lookups().lookupTopic(topic1);
        String serviceUrlForTopic2 = pulsarAdmins[0].lookups().lookupTopic(topic2);

        while (serviceUrlForTopic1.equals(serviceUrlForTopic2)) {
            // Retry for bundle distribution, should make sure bundles for topic1 and topic2 are maintained in different brokers.
            pulsarAdmins[0].namespaces().unload("my-tenant/my-ns");
            serviceUrlForTopic1 = pulsarAdmins[0].lookups().lookupTopic(topic1);
            serviceUrlForTopic2 = pulsarAdmins[0].lookups().lookupTopic(topic2);
        }
        // All brokers will invalidate bundles cache after namespace bundle split
        pulsarAdmins[0].namespaces().splitNamespaceBundle("my-tenant/my-ns",
                pulsarServices[0].getNamespaceService().getBundle(TopicName.get(topic1)).getBundleRange(),
                true, null);

        PulsarClient client = PulsarClient.builder().
                serviceUrl(serviceUrlForTopic1)
                .build();

        // Check connect to a topic which owner broker invalidate all namespace bundles cache
        Consumer<byte[]> consumer = client.newConsumer().topic(topic2).subscriptionName("test").subscribe();
        Assert.assertTrue(consumer.isConnected());
    }
}
