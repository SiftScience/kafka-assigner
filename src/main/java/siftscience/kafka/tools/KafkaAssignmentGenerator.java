package siftscience.kafka.tools;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Prints assignments of topic partition replicas to brokers.
 * <br />
 * The output format is JSON, and can be directly fed into the Kafka partition assigner tool.
 * <br />
 * Usage:
 * <code>
 *     bin/kafka-assignment-generator.sh
 *     --bootstrap_servers host1:9092
 *     --mode PRINT_REASSIGNMENT
 *     --broker_hosts host1,host2,host3
 *     --broker_hosts_to_remove misbehaving_host1
 * </code>
 */
public class KafkaAssignmentGenerator {
    private static final int KAFKA_FORMAT_VERSION = 1;

    private static final Splitter SPLITTER = Splitter.on(',');

    @Option(name = "--bootstrap_server",
            usage = "Kafka Broker(s) to bootstrap connection (comma-separated host:port pairs)")
    private String bootstrapServers = null;

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

    private static void printCurrentAssignment(AdminClient adminClient, List<String> specifiedTopics) throws ExecutionException, InterruptedException {
        List<String> topics = specifiedTopics != null ? specifiedTopics : new ArrayList<>(adminClient.listTopics().names().get());
        // Sort the topics for nice, diff-able output
        Collections.sort(topics);
        Map<String, Map<Integer, List<Integer>>> partitionAssignment = getPartitionAssignment(adminClient, topics);

        JSONObject json = new JSONObject();
        json.put("version", KAFKA_FORMAT_VERSION);
        JSONArray partitionsJson = new JSONArray();
        for (String topic : topics) {
            Map<Integer, List<Integer>> partitionToReplicas = partitionAssignment.get(topic);
            for (Integer partition : partitionToReplicas.keySet()) {
                JSONObject partitionJson = new JSONObject();
                partitionJson.put("topic", topic);
                partitionJson.put("partition", partition);
                partitionJson.put("replicas", new JSONArray(partitionToReplicas.get(partition)));
                partitionsJson.put(partitionJson);
            }
        }
        json.put("partitions", partitionsJson);

        System.out.println("CURRENT ASSIGNMENT:");
        System.out.println(json);
    }

    private static void printCurrentBrokers(AdminClient adminClient) throws JSONException, ExecutionException, InterruptedException {
        JSONArray json = new JSONArray();
        for (Node node: adminClient.describeCluster().nodes().get()) {
            JSONObject brokerJson = new JSONObject();
            brokerJson.put("id", node.id());
            brokerJson.put("host", node.host());
            brokerJson.put("port", node.port());
            brokerJson.put("rack", node.rack());
            json.put(brokerJson);
        }
        System.out.println("CURRENT BROKERS:");
        System.out.println(json.toString());
    }

    /**
     * @return map from topic name to a map from partition ID to broker IDs that partition has been assigned to.
     */
    private static Map<String, Map<Integer, List<Integer>>> getPartitionAssignment(AdminClient adminClient, List<String> topics) throws ExecutionException, InterruptedException {
        Map<String, TopicDescription> topicDescriptions = adminClient.describeTopics(topics).all().get();
        Map<String, Map<Integer, List<Integer>>> result = new HashMap<>();
        for (TopicDescription topicDescription : topicDescriptions.values()) {
            Map<Integer, List<Integer>> partitionToReplicas = new HashMap<>();
            for (TopicPartitionInfo tp : topicDescription.partitions()) {
                List<Integer> replicas = new ArrayList<>(Lists.transform(tp.replicas(), new Function<Node, Integer>() {
                    @Override
                    public Integer apply(Node node) {
                        return node.id();
                    }
                }));
                partitionToReplicas.put(tp.partition(), replicas);
            }
            result.put(topicDescription.name(), partitionToReplicas);
        }
        return result;
    }

    private static void printLeastDisruptiveReassignment(
            AdminClient adminClient, List<String> specifiedTopics, Set<Integer> specifiedBrokers,
            Set<Integer> excludedBrokers, Map<Integer, String> rackAssignment, int desiredReplicationFactor)
            throws JSONException, ExecutionException, InterruptedException {
        // We need three inputs for rebalacing: the brokers, the topics, and the current assignment
        // of topics to brokers.
        Set<Integer> brokerSet = specifiedBrokers;
        if (brokerSet == null || brokerSet.isEmpty()) {
            brokerSet = Sets.newHashSet(Collections2.transform(adminClient.describeCluster().nodes().get(),
                    new Function<Node, Integer>() {
                        public Integer apply(Node node) {
                            return node.id();
                        }
                    }));
        }

        // Exclude brokers that we want to decommission
        Set<Integer> brokers = Sets.difference(brokerSet, excludedBrokers);
        rackAssignment.keySet().retainAll(brokers);

        // The most common use case is to rebalance all topics, but explicit topic addition is also supported.
        List<String> topics = specifiedTopics != null ? specifiedTopics : new ArrayList<>(adminClient.listTopics().names().get());
        // Sort the topics for nice, diff-able output
        Collections.sort(topics);

        // Print the current assignment in case a rollback is needed
        printCurrentAssignment(adminClient, topics);

        Map<String, Map<Integer, List<Integer>>> initialAssignments = getPartitionAssignment(adminClient, topics);

        // Assign topics one at a time. This is slightly suboptimal from a packing standpoint, but
        // it's close enough to work in practice. We can also always follow it up with a Kafka
        // leader election rebalance if necessary.
        JSONObject json = new JSONObject();
        json.put("version", KAFKA_FORMAT_VERSION);
        JSONArray partitionsJson = new JSONArray();
        KafkaTopicAssigner assigner = new KafkaTopicAssigner();
        for (String topic : topics) {
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
            AdminClient adminClient, Set<String> brokerHostnameSet, boolean checkPresence)throws ExecutionException, InterruptedException {

        Collection<Node> nodes = adminClient.describeCluster().nodes().get();
        Set<Integer> brokerIdSet = Sets.newHashSet();
        for (Node node : nodes) {
            if (brokerHostnameSet.contains(node.host())) {
                brokerIdSet.add(node.id());
                System.out.println(node.id());
            }
        }
        Preconditions.checkArgument(!checkPresence ||
                brokerHostnameSet.size() == brokerIdSet.size(),
                "Some hostnames could not be found! We found: " + brokerIdSet);

        return brokerIdSet;
    }

    private Set<Integer> getBrokerIds(AdminClient adminClient) throws ExecutionException, InterruptedException {
        Set<Integer> brokerIdSet = Collections.emptySet();
        if (StringUtils.isNotEmpty(brokerIds)) {
            brokerIdSet = ImmutableSet.copyOf(Iterables.transform(SPLITTER
                    .split(brokerIds), new Function<String, Integer>() {
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
            brokerIdSet = brokerHostnamesToBrokerIds(adminClient, brokerHostnameSet, true);
        }
        return brokerIdSet;
    }

    private Set<Integer> getExcludedBrokerIds(AdminClient adminClient) throws ExecutionException, InterruptedException {
        if (StringUtils.isNotEmpty(brokerHostnamesToReplace)) {
            Set<String> brokerHostnamesToReplaceSet = ImmutableSet.copyOf(
                    SPLITTER.split(brokerHostnamesToReplace));
            return ImmutableSet.copyOf(
                    brokerHostnamesToBrokerIds(adminClient, brokerHostnamesToReplaceSet, false));

        }
        return Collections.emptySet();
    }

    private Map<Integer, String> getRackAssignment(AdminClient adminClient) throws ExecutionException, InterruptedException {
        Map<Integer, String> rackAssignment = Maps.newHashMap();
        if (!disableRackAwareness) {
            for (Node node : adminClient.describeCluster().nodes().get()) {
                if (node.hasRack()) {
                    rackAssignment.put(node.id(), node.rack());
                }
            }
        }
        return rackAssignment;
    }

    private List<String> getTopics() {
        return topics != null ? Lists.newLinkedList(SPLITTER.split(topics)) : null;
    }

    private void runTool(String[] args) throws JSONException, ExecutionException, InterruptedException {
        // Get and validate all arguments from args4j
        CmdLineParser parser = new CmdLineParser(this);
        try {
            parser.parseArgument(args);
            Preconditions.checkNotNull(bootstrapServers);
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
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);

        try (AdminClient adminClient = AdminClient.create(props)) {
            Set<Integer> brokerIdSet = getBrokerIds(adminClient);
            Set<Integer> excludedBrokerIdSet = getExcludedBrokerIds(adminClient);
            Map<Integer, String> rackAssignment = getRackAssignment(adminClient);
            switch (mode) {
                case PRINT_CURRENT_ASSIGNMENT:
                    printCurrentAssignment(adminClient, topics);
                    break;
                case PRINT_CURRENT_BROKERS:
                    printCurrentBrokers(adminClient);
                    break;
                case PRINT_REASSIGNMENT:
                    printLeastDisruptiveReassignment(adminClient, topics, brokerIdSet,
                            excludedBrokerIdSet, rackAssignment, desiredReplicationFactor);
                    break;
                default:
                    throw new UnsupportedOperationException("Invalid mode: " + mode);
            }
        }
    }

    public static void main(String[] args) throws JSONException, ExecutionException, InterruptedException {
        new KafkaAssignmentGenerator().runTool(args);
    }
}
