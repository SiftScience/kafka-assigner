package com.sift.kafka.tools.partitionassigner;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 * Keep track of which replica IDs need more coverage for this topic.
 */
public class PreferenceListOrderTracker {
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
        List<Integer> nodeProcessingOrder = new NodeProcessingOrder(topicName).get(nodes);
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
        Map<Integer, Integer> replicaCount =
                nodeAssignmentCounters.computeIfAbsent(nodeId, k -> Maps.newHashMap());
        return replicaCount.computeIfAbsent(replicaId, k -> 0);
    }
}
