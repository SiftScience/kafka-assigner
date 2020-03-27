package com.sift.kafka.tools.partitionassigner;

import java.util.List;
import java.util.Map;

public interface AssignmentStrategy {

    Map<Integer, List<Integer>> generateAssignment(String topic, Map<Integer, List<Integer>> currentAssignment);
}
