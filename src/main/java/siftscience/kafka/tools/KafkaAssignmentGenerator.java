package siftscience.kafka.tools;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import kafka.cluster.Broker;
import kafka.cluster.BrokerEndPoint;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;

import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import scala.collection.JavaConversions;
import scala.collection.Seq;

/**
 * Prints assignments of topic partition replicas to brokers.
 * <br />
 * The output format is JSON, and can be directly fed into the Kafka partition assigner tool.
 * <br />
 * Usage:
 * <code>
 *     bin/kafka-assignment-generator.sh
 *     --zk_string zkhost:2181
 *     --mode PRINT_REASSIGNMENT
 *     --broker_hosts host1,host2,host3
 *     --broker_hosts_to_remove misbehaving_host1
 * </code>
 */
public class KafkaAssignmentGenerator {
    private static final int KAFKA_FORMAT_VERSION = 1;

    private static final Splitter SPLITTER = Splitter.on(',');

    @Option(name = "--zk_string",
            usage = "ZK quorum as comma-separated host:port pairs")
    private String zkConnectString = null;

    @Option(name = "--mode",
            usage = "the mode to run (PRINT_CURRENT_ASSIGNMENT, PRINT_CURRENT_BROKERS, " +
                    "PRINT_REASSIGNMENT)")
    private Mode mode = null;

    @Option(name = "--integer_broker_ids",
            usage = "comma-separated list of Kafka broker IDs (integers)")
    private String brokerIds = null;

    @Option(name = "--broker_hosts",
            usage = "comma-separated list of broker hostnames (instead of broker IDs)")
    private String brokerHostnames = null;

    @Option(name = "--broker_hosts_to_remove",
            usage = "comma-separated list of broker hostnames to exclude (instead of broker IDs)")
    private String brokerHostnamesToReplace = null;

    @Option(name = "--topics",
            usage = "comma-separated list of topics")
    private String topics = null;

    @Option(name = "--desired_replication_factor",
            usage = "used for changing replication factor for topics, if not present it will use the existing number")
    private int desiredReplicationFactor = -1;

    @Option(name = "--disable_rack_awareness",
            usage = "set to true to ignore rack configurations")
    private boolean disableRackAwareness = false;

    private enum Mode {
        /**
         * Get a snapshot of assigned partitions in Kafka-parseable JSON.
         */
        PRINT_CURRENT_ASSIGNMENT,

        /**
         * Get a list of brokers in the cluster, including their Kafka ID and host:port.
         */
        PRINT_CURRENT_BROKERS,

        /**
         * Get a near-optimal reassignment with minimal partition movement.
         */
        PRINT_REASSIGNMENT
    }

    private static void printCurrentAssignment(ZkUtils zkUtils, List<String> specifiedTopics) {
        Seq<String> topics = specifiedTopics != null ?
                JavaConversions.iterableAsScalaIterable(specifiedTopics).toSeq() :
                zkUtils.getAllTopics();
        System.out.println("CURRENT ASSIGNMENT:");
        System.out.println(
                zkUtils.formatAsReassignmentJson(zkUtils.getReplicaAssignmentForTopics(
                        topics)));
    }

    private static void printCurrentBrokers(ZkUtils zkUtils) throws JSONException {
        List<Broker> brokers = JavaConversions.seqAsJavaList(zkUtils.getAllBrokersInCluster());
        JSONArray json = new JSONArray();
        for (Broker broker : brokers) {
            BrokerEndPoint endpoint = broker.getBrokerEndPoint(SecurityProtocol.PLAINTEXT);
            JSONObject brokerJson = new JSONObject();
            brokerJson.put("id", broker.id());
            brokerJson.put("host", endpoint.host());
            brokerJson.put("port", endpoint.port());
            if (broker.rack().isDefined()) {
                brokerJson.put("rack", broker.rack().get());
            }
            json.put(brokerJson);
        }
        System.out.println("CURRENT BROKERS:");
        System.out.println(json.toString());
    }

    private static void printLeastDisruptiveReassignment(
            ZkUtils zkUtils, List<String> specifiedTopics, Set<Integer> specifiedBrokers,
            Set<Integer> excludedBrokers, Map<Integer, String> rackAssignment, int desiredReplicationFactor)
            throws JSONException {
        // We need three inputs for rebalacing: the brokers, the topics, and the current assignment
        // of topics to brokers.
        Set<Integer> brokerSet = specifiedBrokers;
        if (brokerSet == null || brokerSet.isEmpty()) {
            brokerSet = Sets.newHashSet(Lists.transform(
                    JavaConversions.seqAsJavaList(zkUtils.getAllBrokersInCluster()),
                    new Function<Broker, Integer>() {
                        @Override
                        public Integer apply(Broker broker) {
                            return broker.id();
                        }
                    }));
        }

        // Exclude brokers that we want to decommission
        Set<Integer> brokers = Sets.difference(brokerSet, excludedBrokers);
        rackAssignment.keySet().retainAll(brokers);

        // The most common use case is to rebalance all topics, but explicit topic addition is also
        // supported.
        Seq<String> topics = specifiedTopics != null ?
                JavaConversions.collectionAsScalaIterable(specifiedTopics).toSeq() :
                zkUtils.getAllTopics();

        // Print the current assignment in case a rollback is needed
        printCurrentAssignment(zkUtils, JavaConversions.seqAsJavaList(topics));

        Map<String, Map<Integer, List<Integer>>> initialAssignments =
                KafkaTopicAssigner.topicMapToJavaMap(zkUtils.getPartitionAssignmentForTopics(
                        topics));

        // Assign topics one at a time. This is slightly suboptimal from a packing standpoint, but
        // it's close enough to work in practice. We can also always follow it up with a Kafka
        // leader election rebalance if necessary.
        JSONObject json = new JSONObject();
        json.put("version", KAFKA_FORMAT_VERSION);
        JSONArray partitionsJson = new JSONArray();
        KafkaTopicAssigner assigner = new KafkaTopicAssigner();
        for (String topic : JavaConversions.seqAsJavaList(topics)) {
            Map<Integer, List<Integer>> partitionAssignment = initialAssignments.get(topic);
            Map<Integer, List<Integer>> finalAssignment = assigner.generateAssignment(
                    topic, partitionAssignment, brokers, rackAssignment, desiredReplicationFactor);
            for (Map.Entry<Integer, List<Integer>> e : finalAssignment.entrySet()) {
                JSONObject partitionJson = new JSONObject();
                partitionJson.put("topic", topic);
                partitionJson.put("partition", e.getKey());
                partitionJson.put("replicas", new JSONArray(e.getValue()));
                partitionsJson.put(partitionJson);
            }
        }
        json.put("partitions", partitionsJson);
        System.out.println("NEW ASSIGNMENT:\n" + json.toString());
    }

    private static Set<Integer> brokerHostnamesToBrokerIds(
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

    private Set<Integer> getBrokerIds(ZkUtils zkUtils) {
        Set<Integer> brokerIdSet = Collections.emptySet();
        if (StringUtils.isNotEmpty(brokerIds)) {
            brokerIdSet = ImmutableSet.copyOf(Iterables.transform(SPLITTER
                    .split(brokerIds), new Function<String, Integer>() {
                @Override
                public Integer apply(String brokerId) {
                    try {
                        return Integer.parseInt(brokerId);
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException("Invalid broker ID: " + brokerId);
                    }
                }
            }));
        } else if (StringUtils.isNotEmpty(brokerHostnames)) {
            Set<String> brokerHostnameSet = ImmutableSet.copyOf(SPLITTER.split(brokerHostnames));
            brokerIdSet = brokerHostnamesToBrokerIds(zkUtils, brokerHostnameSet, true);
        }
        return brokerIdSet;
    }

    private Set<Integer> getExcludedBrokerIds(ZkUtils zkUtils) {
        if (StringUtils.isNotEmpty(brokerHostnamesToReplace)) {
            Set<String> brokerHostnamesToReplaceSet = ImmutableSet.copyOf(
                    SPLITTER.split(brokerHostnamesToReplace));
            return ImmutableSet.copyOf(
                    brokerHostnamesToBrokerIds(zkUtils, brokerHostnamesToReplaceSet, false));

        }
        return Collections.emptySet();
    }

    private Map<Integer, String> getRackAssignment(ZkUtils zkUtils) {
        List<Broker> brokers = JavaConversions.seqAsJavaList(zkUtils.getAllBrokersInCluster());
        Map<Integer, String> rackAssignment = Maps.newHashMap();
        if (!disableRackAwareness) {
            for (Broker broker : brokers) {
                scala.Option<String> rack = broker.rack();
                if (rack.isDefined()) {
                    rackAssignment.put(broker.id(), rack.get());
                }
            }
        }
        return rackAssignment;
    }

    private List<String> getTopics() {
        return topics != null ? Lists.newLinkedList(SPLITTER.split(topics)) : null;
    }

    private void runTool(String[] args) throws JSONException {
        // Get and validate all arguments from args4j
        CmdLineParser parser = new CmdLineParser(this);
        try {
            parser.parseArgument(args);
            Preconditions.checkNotNull(zkConnectString);
            Preconditions.checkNotNull(mode);
            Preconditions.checkArgument(brokerIds == null || brokerHostnames == null,
                    "--kafka_assigner_integer_broker_ids and " +
                            "--kafka_assigner_broker_hosts cannot be used together!");
        } catch (Exception e) {
            System.err.println("./kafka-assignment-generator.sh [options...] arguments...");
            parser.printUsage(System.err);
            return;
        }
        List<String> topics = getTopics();

        ZkClient zkClient = new ZkClient(zkConnectString, 10000, 10000,
                ZKStringSerializer$.MODULE$);
        zkClient.waitUntilConnected();
        ZkUtils zkUtils = ZkUtils.apply(zkClient, false);

        try {
            Set<Integer> brokerIdSet = getBrokerIds(zkUtils);
            Set<Integer> excludedBrokerIdSet = getExcludedBrokerIds(zkUtils);
            Map<Integer, String> rackAssignment = getRackAssignment(zkUtils);
            switch (mode) {
                case PRINT_CURRENT_ASSIGNMENT:
                    printCurrentAssignment(zkUtils, topics);
                    break;
                case PRINT_CURRENT_BROKERS:
                    printCurrentBrokers(zkUtils);
                    break;
                case PRINT_REASSIGNMENT:
                    printLeastDisruptiveReassignment(zkUtils, topics, brokerIdSet,
                            excludedBrokerIdSet, rackAssignment, desiredReplicationFactor);
                    break;
                default:
                    throw new UnsupportedOperationException("Invalid mode: " + mode);
            }
        } finally {
            zkUtils.close();
        }
    }

    public static void main(String[] args) throws JSONException {
        new KafkaAssignmentGenerator().runTool(args);
    }
}
