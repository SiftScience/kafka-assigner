package com.sift.kafka.tools.partitionassigner;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class NodeProcessingOrder {

    private final String topic;

    public NodeProcessingOrder(String topic) {
        this.topic = topic;
    }

    public List<Integer> get(Map<Integer, Node> nodeMap) {
        return get(nodeMap.keySet());
    }

    public List<Integer> get(Collection<Integer> nodeIds) {
        Integer[] nodeProcessingOrder = new Integer[nodeIds.size()];
        int index = Math.abs(topic.hashCode()) % nodeProcessingOrder.length;
        for (int nodeId : nodeIds) {
            nodeProcessingOrder[index] = nodeId;
            // move the pointer forward, but wrap around if at the end
            if (++index == nodeProcessingOrder.length) {
                index = 0;
            }
        }
        return Arrays.asList(nodeProcessingOrder);
    }
}
