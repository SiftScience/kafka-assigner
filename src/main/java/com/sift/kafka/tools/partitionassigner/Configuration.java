package com.sift.kafka.tools.partitionassigner;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.*;
import kafka.cluster.Broker;
import kafka.cluster.BrokerEndPoint;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import scala.collection.JavaConversions;
import siftscience.kafka.tools.KafkaAssignmentGenerator;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Configuration {

    private static final Splitter SPLITTER = Splitter.on(',');

    private final String[] args;

    @Option(name = "--broker_hosts",
            usage = "comma-separated list of broker hostnames (instead of broker IDs)")
    private String brokerHostnames = null;

    @Option(name = "--broker_hosts_to_remove",
            usage = "comma-separated list of broker hostnames to exclude (instead of broker IDs)")
    private String brokerHostnamesToReplace = null;

    @Option(name = "--integer_broker_ids",
            usage = "comma-separated list of Kafka broker IDs (integers)")
    private String brokerIds = null;

    @Option(name = "--desired_replication_factor",
            usage = "used for changing replication factor for topics, if not present it will use the existing number")
    private int desiredReplicationFactor = -1;

    @Option(name = "--mode",
            usage = "the mode to run (PRINT_CURRENT_ASSIGNMENT, PRINT_CURRENT_BROKERS, " +
                    "PRINT_REASSIGNMENT)")
    private KafkaAssignmentGenerator.Mode mode = null;

    @Option(name = "--is_rack_aware",
            usage = "set to false to ignore rack configurations")
    private boolean rackAware = true;

    @Option(name = "--topics",
            usage = "comma-separated list of topics")
    private String topics = null;

    @Option(name = "--zk_string",
            usage = "ZK quorum as comma-separated host:port pairs")
    private String zkConnectString = null;

    public Configuration(String[] args) {
        this.args = args;
    }

    private Set<Integer> brokerHostnamesToBrokerIds(
            ZkUtils zkUtils, Set<String> brokerHostnameSet, boolean checkPresence) {
        List<Broker> brokers = JavaConversions.seqAsJavaList(zkUtils.getAllBrokersInCluster());
        Set<Integer> brokerIdSet = Sets.newHashSet();
        for (Broker broker : brokers) {
            BrokerEndPoint endpoint = broker.getBrokerEndPoint(SecurityProtocol.PLAINTEXT);
            if (brokerHostnameSet.contains(endpoint.host())) {
                brokerIdSet.add(broker.id());
            }
        }
        Preconditions.checkArgument(!checkPresence ||
                        brokerHostnameSet.size() == brokerIdSet.size(),
                "Some hostnames could not be found! We found: " + brokerIdSet);

        return brokerIdSet;
    }

    public Set<Integer> getBrokerIds(ZkUtils zkUtils) {
        Set<Integer> brokerIdSet = Collections.emptySet();
        if (StringUtils.isNotEmpty(brokerIds)) {
            brokerIdSet = ImmutableSet.copyOf(
                    StreamSupport.stream(SPLITTER.split(brokerIds).spliterator(), false).map(brokerId -> {
                try {
                    return Integer.parseInt(brokerId);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid broker ID: " + brokerId);
                }
            }).collect(Collectors.toList()));
        } else if (StringUtils.isNotEmpty(brokerHostnames)) {
            Set<String> brokerHostnameSet = ImmutableSet.copyOf(SPLITTER.split(brokerHostnames));
            brokerIdSet = brokerHostnamesToBrokerIds(zkUtils, brokerHostnameSet, true);
        }
        return brokerIdSet;
    }

    public int getDesiredReplicationFactor() {
        return desiredReplicationFactor;
    }

    public Set<Integer> getExcludedBrokerIds(ZkUtils zkUtils) {
        if (StringUtils.isNotEmpty(brokerHostnamesToReplace)) {
            Set<String> brokerHostnamesToReplaceSet = ImmutableSet.copyOf(
                    SPLITTER.split(brokerHostnamesToReplace));
            return ImmutableSet.copyOf(
                    brokerHostnamesToBrokerIds(zkUtils, brokerHostnamesToReplaceSet, false));

        }
        return Collections.emptySet();
    }

    public KafkaAssignmentGenerator.Mode getMode() {
        return mode;
    }

    public Map<Integer, String> getRackAssignment(ZkUtils zkUtils) {
        List<Broker> brokers = JavaConversions.seqAsJavaList(zkUtils.getAllBrokersInCluster());
        Map<Integer, String> rackAssignment = Maps.newHashMap();
        if (rackAware) {
            for (Broker broker : brokers) {
                scala.Option<String> rack = broker.rack();
                if (rack.isDefined()) {
                    rackAssignment.put(broker.id(), rack.get());
                }
            }
        }
        return rackAssignment;
    }

    public List<String> getTopics() {
        return topics != null ? Lists.newLinkedList(SPLITTER.split(topics)) : null;
    }

    public ZkUtils getZkUtils() {
        ZkClient zkClient = new ZkClient(zkConnectString, 10000, 10000,
                ZKStringSerializer$.MODULE$);
        zkClient.waitUntilConnected();
        return ZkUtils.apply(zkClient, false);
    }

    public void validate() {
        CmdLineParser parser = new CmdLineParser(this);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println("./kafka-assignment-generator.sh [options...] arguments...");
            parser.printUsage(System.err);
            return;
        }

        Preconditions.checkNotNull(zkConnectString);
        Preconditions.checkNotNull(mode);
        Preconditions.checkArgument(brokerIds == null || brokerHostnames == null,
                "--kafka_assigner_integer_broker_ids and " +
                        "--kafka_assigner_broker_hosts cannot be used together!");
    }
}
