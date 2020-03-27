package siftscience.kafka.tools;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.sift.kafka.tools.partitionassigner.Configuration;
import com.sift.kafka.tools.partitionassigner.TopicMapSupplier;

import kafka.cluster.Broker;
import kafka.cluster.BrokerEndPoint;
import kafka.utils.ZkUtils;

import org.apache.kafka.common.protocol.SecurityProtocol;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

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

    public enum Mode {
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

    private static void printLeastDisruptiveReassignment(ZkUtils zkUtils,
                                                         List<String> specifiedTopics,
                                                         Configuration configuration)
            throws JSONException {
        Set<Integer> specifiedBrokers = configuration.getBrokerIds(zkUtils);
        Set<Integer> excludedBrokers = configuration.getExcludedBrokerIds(zkUtils);
        int replicationFactor = configuration.getDesiredReplicationFactor();
        Map<Integer, String> rackAssignment = configuration.getRackAssignment(zkUtils);

        // We need three inputs for rebalancing: the brokers, the topics, and the current assignment
        // of topics to brokers.
        Set<Integer> brokerSet = specifiedBrokers;
        if (brokerSet == null || brokerSet.isEmpty()) {
            brokerSet = Sets.newHashSet(Lists.transform(
                    JavaConversions.seqAsJavaList(zkUtils.getAllBrokersInCluster()), Broker::id));
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
                new TopicMapSupplier(zkUtils.getPartitionAssignmentForTopics(topics)).get();

        // Assign topics one at a time. This is slightly suboptimal from a packing standpoint, but
        // it's close enough to work in practice. We can also always follow it up with a Kafka
        // leader election rebalance if necessary.
        JSONObject json = new JSONObject();
        json.put("version", KAFKA_FORMAT_VERSION);
        JSONArray partitionsJson = new JSONArray();
        MinimalMovementStrategy assigner =
                new MinimalMovementStrategy(brokers, rackAssignment, replicationFactor);
        for (String topic : JavaConversions.seqAsJavaList(topics)) {
            Map<Integer, List<Integer>> partitionAssignment = initialAssignments.get(topic);
            //this is the key line that generates the final assignment from the
            Map<Integer, List<Integer>> finalAssignment =
                    assigner.generateAssignment(topic, partitionAssignment);
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

    private void runTool(String[] args) throws JSONException {

        Configuration configuration = new Configuration(args);
        configuration.validate();

        ZkUtils zkUtils = configuration.getZkUtils();
        List<String> topics = configuration.getTopics();

        Mode mode = configuration.getMode();
        try {
            switch (mode) {
                case PRINT_CURRENT_ASSIGNMENT:
                    printCurrentAssignment(zkUtils, topics);
                    break;
                case PRINT_CURRENT_BROKERS:
                    printCurrentBrokers(zkUtils);
                    break;
                case PRINT_REASSIGNMENT:
                    printLeastDisruptiveReassignment(zkUtils, topics, configuration);
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
