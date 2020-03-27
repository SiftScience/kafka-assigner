package com.sift.kafka.tools.partitionassigner;

import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

public class Rack {
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
