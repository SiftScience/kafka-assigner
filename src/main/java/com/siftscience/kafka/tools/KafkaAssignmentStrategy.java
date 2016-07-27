package siftscience.kafka.tools;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * A Kafka-specific assignment strategy that takes into account rack-awareness.
 * <br />
 * This is similar to Apache Helix's AutoRebalanceStrategy, but more specialized, and also strictly
 * prevents assignment of multiple replicas of a partition to the same rack. Note that this means
 * that the replication factor of any topic should be less than the number of available racks.
 * <br />
 * Attribution: Apache Helix is licensed under version 2.0 of the Apache License.
 */
public class KafkaAssignmentStrategy {
    /**
     * Compute a new rack-aware assignment given an existing assignment.
     * <br />
     * A known issue with this algorithm is that it may not find a valid assignment if there is not
     * an equal number of nodes on each rack. Adding additional nodes will remedy this issue.
     * @param topicName name of the topic (for sharding/balancing)
     * @param currentAssignment map of partition to list of nodes currently serving it
     * @param nodeRackAssignment map of node id to the rack id that owns the node
     * @param nodes set of node ids that can accept partitions
     * @param partitions set of partition ids for a topic
     * @param replicationFactor number of replicas per partition to assign
     * @param context an object to allow state to be maintained across calls, or null
     * @return a map from partition to node ids ordered by leadership preference
     */
    public static Map<Integer, List<Integer>> getRackAwareAssignment(
            String topicName, Map<Integer, List<Integer>> currentAssignment,
            Map<Integer, String> nodeRackAssignment, Set<Integer> nodes, Set<Integer> partitions,
            int replicationFactor, Context context) {
        // Initialize nodes with capacities and nothing assigned
        int maxReplicas = getMaxReplicasPerNode(nodes, partitions, replicationFactor);
        SortedMap<Integer, Node> nodeMap = createNodeMap(nodeRackAssignment, nodes, maxReplicas);

        // Using the current assignment, reassign as many partitions as each node can accept
        fillNodesFromAssignment(currentAssignment, nodeMap);

        // Figure out the replicas that have not been assigned yet
        Map<Integer, Integer> orphanedReplicas = getOrphanedReplicas(nodeMap, partitions,
                replicationFactor);

        // Assign those replicas to nodes that can accept them
        assignOrphans(topicName, nodeMap, orphanedReplicas);

        // Order nodes for each partition such that leadership is relatively balanced
        if (context == null) {
            context = new Context();
        }
        return computePreferenceLists(topicName, nodeMap, context);
    }

    private static int getMaxReplicasPerNode(
            Set<Integer> nodes, Set<Integer> partitions, int replicationFactor) {
        // The capacity of a node is the number of partitions times the number of replicas divided
        // by the number of nodes (add one if it doesn't divide evenly)
        double totalReplicas = partitions.size() * replicationFactor;
        return (int) Math.ceil(totalReplicas / nodes.size());
    }

    private static SortedMap<Integer, Node> createNodeMap(
            Map<Integer, String> nodeRackAssignment, Set<Integer> nodes, int maxReplicas) {
        // Create empty nodes with the specified capacity. Also create rack objects so that node
        // assignment can also look at the rack.
        Map<String, Rack> rackMap = Maps.newTreeMap();
        SortedMap<Integer, Node> nodeMap = Maps.newTreeMap();
        for (Integer nodeId : nodes) {
            Preconditions.checkState(!nodeMap.containsKey(nodeId));
            String rackId = nodeRackAssignment.get(nodeId);
            if (rackId == null) {
                // Use the node id as the rack id if there isn't a rack for the node. This allows
                // this algorithm to work correctly even if we don't care about rack awareness.
                rackId = nodeId.toString();
            }

            // Reuse the rack object if we've seen a node on this rack before so that we can track
            // assignments to a rack together.
            Rack rack = rackMap.get(rackId);
            if (rack == null) {
                rack = new Rack(rackId);
                rackMap.put(rackId, rack);
            }
            Node node = new Node(nodeId, maxReplicas, rack);
            nodeMap.put(nodeId, node);
        }
        return nodeMap;
    }

    private static void fillNodesFromAssignment(
            Map<Integer, List<Integer>> assignment, Map<Integer, Node> nodeMap) {
        // Assign existing partitions back to nodes in a round-robin fashion. This ensures that
        // we prevent (when possible) multiple replicas of the same partition moving around in the
        // cluster at the same time. It also helps ensure that we have orphaned replicas that nodes
        // can accept.
        Map<Integer, Iterator<Integer>> assignmentIterators = Maps.newTreeMap();
        for (Map.Entry<Integer, List<Integer>> e : assignment.entrySet()) {
            assignmentIterators.put(e.getKey(), e.getValue().iterator());
        }
        boolean filled = false;
        while (!filled) {
            Iterator<Integer> roundRobin = assignmentIterators.keySet().iterator();
            while (roundRobin.hasNext()) {
                int partition = roundRobin.next();
                Iterator<Integer> nodeIt = assignmentIterators.get(partition);
                if (nodeIt.hasNext()) {
                    int nodeId = nodeIt.next();
                    Node node = nodeMap.get(nodeId);
                    if (node != null && node.canAccept(partition)) {
                        // The node from the current assignment must still exist and be able to
                        // accept the partition.
                        node.accept(partition);
                    }
                } else {
                    roundRobin.remove();
                }
            }
            filled = assignmentIterators.isEmpty();
        }
    }

    private static Map<Integer, Integer> getOrphanedReplicas(
            Map<Integer, Node> nodeMap, Set<Integer> partitions, int replicationFactor) {
        // Get the number of assigned replicas per partition
        Map<Integer, Integer> partitionCounter = Maps.newTreeMap();
        for (Node node : nodeMap.values()) {
            for (int partition : node.assignedPartitions) {
                if (!partitionCounter.containsKey(partition)) {
                    partitionCounter.put(partition, 1);
                } else {
                    partitionCounter.put(partition, partitionCounter.get(partition) + 1);
                }
            }
        }

        // Using the above information, and the replication factor, get the number of unassigned
        // replicas per partition
        Map<Integer, Integer> orphanedReplicas = Maps.newTreeMap();
        for (int partition : partitions) {
            int remainingReplicas = replicationFactor;
            if (partitionCounter.containsKey(partition)) {
                remainingReplicas -= partitionCounter.get(partition);
            }
            if (remainingReplicas > 0) {
                orphanedReplicas.put(partition, remainingReplicas);
            }
        }
        return orphanedReplicas;
    }

    private static void assignOrphans(
            String topicName, SortedMap<Integer, Node> nodeMap,
            Map<Integer, Integer> orphanedReplicas) {
        // Don't process nodes in the same order for all topics to ensure that topics with fewer
        // replicas than nodes are equally likely to be assigned anywhere (and not overload the
        // brokers with earlier IDs).
        Integer[] nodeProcessingOrder = getNodeProcessingOrder(topicName, nodeMap.keySet());
        List<Integer> nodeProcessingOrderList = Arrays.asList(nodeProcessingOrder);

        // Assign unassigned replicas to nodes that can accept them
        for (Map.Entry<Integer, Integer> e : orphanedReplicas.entrySet()) {
            int partition = e.getKey();
            int remainingReplicas = e.getValue();
            Iterator<Integer> nodeIt = nodeProcessingOrderList.iterator();
            while (nodeIt.hasNext() && remainingReplicas > 0) {
                Node node = nodeMap.get(nodeIt.next());
                if (node.canAccept(partition)) {
                    node.accept(partition);
                    remainingReplicas--;
                }
            }
            Preconditions.checkState(remainingReplicas == 0, "Partition " + partition +
                    " could not be fully assigned!");
        }
    }

    private static Integer[] getNodeProcessingOrder(String topicName, Collection<Integer> nodeIds) {
        Integer[] nodeProcessingOrder = new Integer[nodeIds.size()];
        int index = Math.abs(topicName.hashCode()) % nodeProcessingOrder.length;
        for (int nodeId : nodeIds) {
            nodeProcessingOrder[index] = nodeId;

            // move the pointer forward, but wrap around if at the end
            if (++index == nodeProcessingOrder.length) {
                index = 0;
            }
        }
        return nodeProcessingOrder;
    }

    private static Map<Integer, List<Integer>> computePreferenceLists(
            String topicName, Map<Integer, Node> nodeMap, Context context) {
        // First, get unordered assignment lists from the nodes
        Map<Integer, List<Integer>> unorderedPreferences = Maps.newTreeMap();
        for (Node node : nodeMap.values()) {
            int nodeId = node.id;
            for (int partition : node.assignedPartitions) {
                if (!unorderedPreferences.containsKey(partition)) {
                    unorderedPreferences.put(partition, Lists.<Integer>newArrayList());
                }
                unorderedPreferences.get(partition).add(nodeId);
            }
        }

        // Now, order all node lists such that each node is the leader of roughly the same number of
        // partitions
        Map<Integer, Map<Integer, Integer>> nodeAssignmentCounters = context.counter;
        PreferenceListOrderTracker balanceLeadersTracker = new PreferenceListOrderTracker(
                topicName, nodeAssignmentCounters);
        Map<Integer, List<Integer>> preferences = Maps.newTreeMap();
        for (Map.Entry<Integer, List<Integer>> e : unorderedPreferences.entrySet()) {
            int partitionId = e.getKey();
            List<Integer> preferenceList = e.getValue();
            List<Integer> orderedPreferenceList = Lists.newArrayListWithCapacity(
                    preferenceList.size());
            int replicationFactor = preferenceList.size();
            Set<Integer> nodeSet = Sets.newTreeSet(preferenceList);
            for (int replica = 0; replica < replicationFactor; replica++) {
                int nodeToSelect = balanceLeadersTracker.getLeastSeenNodeForReplicaId(replica,
                        nodeSet);
                nodeSet.remove(nodeToSelect);
                orderedPreferenceList.add(nodeToSelect);
            }
            preferences.put(partitionId, orderedPreferenceList);
            balanceLeadersTracker.updateCountersFromList(orderedPreferenceList);
        }
        return preferences;
    }

    /**
     * Keep track of which replica IDs need more coverage for this topic.
     */
    private static class PreferenceListOrderTracker {
        private final String topicName;
        private final Map<Integer, Map<Integer, Integer>> nodeAssignmentCounters;

        public PreferenceListOrderTracker(
                String topicName, Map<Integer, Map<Integer, Integer>> nodeAssignmentCounters) {
            this.topicName = topicName;
            this.nodeAssignmentCounters = nodeAssignmentCounters;
        }

        public void updateCountersFromList(List<Integer> preferences) {
            // Also give fallback leaders a higher weight than the ends of the lists since we want
            // to evenly distribute fallbacks.
            int replica = 0;
            for (int nodeId : preferences) {
                incrementCountSafe(nodeId, replica++);
            }
        }

        public int getLeastSeenNodeForReplicaId(int replicaId, Set<Integer> nodes) {
            // For a given replica, figure out which node of the ones provided is least represented
            Integer minCount = null;
            Integer minNode = null;
            Integer[] nodeProcessingOrder = getNodeProcessingOrder(topicName, nodes);
            for (int nodeId : nodeProcessingOrder) {
                int count = getCountSafe(nodeId, replicaId);
                if (minCount == null || count < minCount) {
                    minCount = count;
                    minNode = nodeId;
                }
            }
            Preconditions.checkNotNull(minCount);
            Preconditions.checkNotNull(minNode);
            return minNode;
        }

        private int getCountSafe(int nodeId, int replicaId) {
            return ensureCount(nodeId, replicaId);
        }

        private void incrementCountSafe(int nodeId, int replicaId) {
            int currentCount = ensureCount(nodeId, replicaId);
            nodeAssignmentCounters.get(nodeId).put(replicaId, currentCount + 1);
        }

        private int ensureCount(int nodeId, int replicaId) {
            Map<Integer, Integer> replicaCount = nodeAssignmentCounters.get(nodeId);
            if (replicaCount == null) {
                replicaCount = Maps.newHashMap();
                nodeAssignmentCounters.put(nodeId, replicaCount);
            }
            Integer currentCount = replicaCount.get(replicaId);
            if (currentCount == null) {
                currentCount = 0;
                replicaCount.put(replicaId, currentCount);
            }
            return currentCount;
        }
    }

    /**
     * Helper class to track assigned partitions and the rack it lives on.
     */
    private static class Node {
        public final int id;
        public final int capacity;
        public final Rack rack;
        public final Set<Integer> assignedPartitions;

        public Node(int id, int capacity, Rack rack) {
            this.id = id;
            this.capacity = capacity;
            this.rack = rack;
            this.assignedPartitions = Sets.newTreeSet();
        }

        public boolean canAccept(int partition) {
            return !assignedPartitions.contains(partition) &&
                    assignedPartitions.size() < capacity &&
                    rack.canAccept(partition);
        }

        public void accept(int partition) {
            Preconditions.checkArgument(canAccept(partition),
                    "Attempted to accept unacceptable partition " + partition);
            assignedPartitions.add(partition);
            rack.accept(partition);
        }
    }

    /**
     * Helper class to track what is assigned to a given rack.
     */
    private static class Rack {
        public final String id;
        public final Set<Integer> assignedPartitions;

        public Rack(String id) {
            this.id = id;
            this.assignedPartitions = Sets.newTreeSet();
        }

        public boolean canAccept(int partition) {
            return !assignedPartitions.contains(partition);
        }

        public void accept(int partition) {
            Preconditions.checkArgument(canAccept(partition),
                    "Attempted to accept unacceptable partition " + partition);
            assignedPartitions.add(partition);
        }
    }

    /**
     * State that can be used to compute better assignments across topics.
     */
    public static class Context {
        private final Map<Integer, Map<Integer, Integer>> counter;

        /**
         * Create an empty context.
         */
        public Context() {
            this.counter = Maps.newHashMap();
        }
    }
}
