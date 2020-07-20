package siftscience.kafka.tools;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Utilities for assigning topic partitions evenly to brokers.
 */
public class KafkaTopicAssigner {
    private final KafkaAssignmentStrategy.Context assignmentContext;

    public KafkaTopicAssigner() {
        this.assignmentContext = new KafkaAssignmentStrategy.Context();
    }

    /**
     * Given a current assignment of partitions, compute a new one that causes minimal movement.
     * <br /><br />
     * The expectation is that if we, for example, replace a node, we should only move partitions
     * from the old node to the new node. If we decommission, we should evenly assign that node's
     * partitions to other nodes without touching anything else. If we add a new node, we should
     * move a fraction of each existing broker's partitions to the new node.
     * @param topic the name of the topic
     * @param currentAssignment A map from partition number to a list of broker ids, representing
     *                          the current assignment of partitions for the topic. The first broker
     *                          in each list is the "leader" replica for a partition.
     * @param brokers a list of broker IDs as strings
     * @param rackAssignment a map from broker ID to rack ID if a rack is defined for that broker
     * @param desiredReplicationFactor used to change replication factor, use -1 to keep the same as
     *                                 the original topic
     * @return the new assignment: a map from partition ID to ordered list of broker IDs
     */
    public Map<Integer, List<Integer>> generateAssignment(
            String topic, Map<Integer, List<Integer>> currentAssignment, Set<Integer> brokers,
            Map<Integer, String> rackAssignment, int desiredReplicationFactor) {
        // We need to do 2 things:
        //  - Get the set of partitions as integers
        //  - Figure out the replication factor (which should be the same for each partition)
        // if desiredReplicationFactor is negative
        int replicationFactor = desiredReplicationFactor;
        Set<Integer> partitions = Sets.newTreeSet();
        for (Map.Entry<Integer, List<Integer>> entry : currentAssignment.entrySet()) {
            int partition = entry.getKey();
            List<Integer> replicas = entry.getValue();
            partitions.add(partition);
            if (replicationFactor < 0) {
                replicationFactor = replicas.size();
            } else if (desiredReplicationFactor < 0) {
                Preconditions.checkState(replicationFactor == replicas.size(),
                        "Topic " + topic + " has partition " + partition +
                                " with unexpected replication factor " + replicas.size());
            }
        }

        // Make sure that we actually managed to process something and get the replication factor
        Preconditions.checkState(replicationFactor > 0, "Topic " + topic +
                " does not have a positive replication factor!");
        Preconditions.checkState(replicationFactor <= brokers.size(), "Topic " + topic +
                " has a higher replication factor (" + replicationFactor +
                ") than available brokers!");
        return KafkaAssignmentStrategy.getRackAwareAssignment(topic, currentAssignment,
                rackAssignment, brokers, partitions, replicationFactor, assignmentContext);
    }
}
