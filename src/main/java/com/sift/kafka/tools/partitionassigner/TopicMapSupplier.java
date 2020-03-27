package com.sift.kafka.tools.partitionassigner;

import com.google.common.collect.Maps;
import scala.collection.JavaConversions;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Convert a Scala Kafka partition assignment into a Java one.
 */
public class TopicMapSupplier implements Supplier<Map<String, Map<Integer, List<Integer>>>> {

    private final scala.collection.Map<String, scala.collection.Map<Object, scala.collection.Seq<Object>>> topicMap;

    /**
     * @param topicMap the output from ZkUtils#getPartitionAssignmentForTopics
     */
    public TopicMapSupplier(scala.collection.Map<String, scala.collection.Map<Object, scala.collection.Seq<Object>>> topicMap) {
        this.topicMap = topicMap;
    }

    /**
     * @return a Java map representing the same data
     */
    @Override
    public Map<String, Map<Integer, List<Integer>>> get() {
        // We can actually use utilities like Maps#transformEntries, but since that doesn't allow
        // changing the key type from Object to Integer, this code just goes into each map and makes
        // copies all the way down. Copying is also useful for avoiding possible repeated lazy
        // evaluations by the rebalancing algorithm.
        Map<String, Map<Integer, List<Integer>>> resultTopicMap = Maps.newHashMap();
        Map<String, scala.collection.Map<Object, scala.collection.Seq<Object>>> convertedTopicMap =
                JavaConversions.mapAsJavaMap(topicMap);
        for (Map.Entry<String, scala.collection.Map<Object,
                scala.collection.Seq<Object>>> topicMapEntry : convertedTopicMap.entrySet()) {
            String topic = topicMapEntry.getKey();
            Map<Object, scala.collection.Seq<Object>> convertedPartitionMap =
                    JavaConversions.mapAsJavaMap(topicMapEntry.getValue());
            Map<Integer, List<Integer>> resultPartitionMap = Maps.newHashMap();
            for (Map.Entry<Object, scala.collection.Seq<Object>> partitionMapEntry :
                    convertedPartitionMap.entrySet()) {
                Integer partition = (Integer) partitionMapEntry.getKey();
                List<Integer> replicaList = JavaConversions.seqAsJavaList(
                        partitionMapEntry.getValue()).stream().map(raw ->
                        (Integer) raw).collect(Collectors.toList());
                resultPartitionMap.put(partition, replicaList);
            }
            resultTopicMap.put(topic, resultPartitionMap);
        }
        return resultTopicMap;
    }
}
