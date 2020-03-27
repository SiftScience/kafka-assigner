package com.sift.kafka.tools.partitionassigner;

import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

/**
 * Helper class to track assigned partitions and the rack it lives on.
 */
public class Node {

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
