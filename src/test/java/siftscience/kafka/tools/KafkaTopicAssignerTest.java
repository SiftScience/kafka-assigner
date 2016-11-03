package siftscience.kafka.tools;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Test;

public class KafkaTopicAssignerTest {
    @Test
    public void testRackAwareExpansion() {
        String topic = "test";
        Map<Integer, List<Integer>> currentAssignment = ImmutableMap.of(
                0, (List<Integer>) ImmutableList.of(10, 11),
                1, ImmutableList.of(11, 12),
                2, ImmutableList.of(12, 10),
                3, ImmutableList.of(10, 12)
        );
        Set<Integer> newBrokers = ImmutableSet.of(10, 11, 12, 13, 14);

        Map<Integer, String> rackAssignments = ImmutableMap.of(
                10, "a",
                11, "b",
                12, "c",
                13, "a",
                14, "b"
        );
        KafkaTopicAssigner assigner = new KafkaTopicAssigner();
        Map<Integer, List<Integer>> newAssignment = assigner.generateAssignment(
                topic, currentAssignment, newBrokers, rackAssignments, -1);

        Map<Integer, Integer> brokerReplicaCounts = verifyPartitionsAndBuildReplicaCounts(
                currentAssignment, newAssignment, 1);

        // Ensure that no broker is doing too much
        // In this case, there are 5 brokers, 4 partitions, and 2 replicas,
        // so there should be 2 brokers serving 1 partition each, and 3 brokers serving 2 each
        int brokersWithOne = 0;
        int brokersWithTwo = 0;
        for (Map.Entry<Integer, Integer> brokerCount : brokerReplicaCounts.entrySet()) {
            if (brokerCount.getValue() == 1) {
                brokersWithOne++;
            } else if (brokerCount.getValue() == 2) {
                brokersWithTwo++;
            }
        }
        Assert.assertEquals(2, brokersWithOne);
        Assert.assertEquals(3, brokersWithTwo);
    }

    @Test
    public void testClusterExpansion() {
        String topic = "test";
        Map<Integer, List<Integer>> currentAssignment = ImmutableMap.of(
                0, (List<Integer>) ImmutableList.of(10, 11),
                1, ImmutableList.of(11, 12),
                2, ImmutableList.of(12, 10),
                3, ImmutableList.of(10, 12)
        );
        Set<Integer> newBrokers = ImmutableSet.of(10, 11, 12, 13);
        KafkaTopicAssigner assigner = new KafkaTopicAssigner();
        Map<Integer, List<Integer>> newAssignment = assigner.generateAssignment(
                topic, currentAssignment, newBrokers, Collections.<Integer, String>emptyMap(), -1);

        Map<Integer, Integer> brokerReplicaCounts = verifyPartitionsAndBuildReplicaCounts(
                currentAssignment, newAssignment, 1);

        // Ensure that no broker is doing too much
        // In this case, there are 4 brokers, 4 partitions, and 2 replicas,
        // so each broker should be serving exactly 2 total partition replicas.
        for (Map.Entry<Integer, Integer> brokerCount : brokerReplicaCounts.entrySet()) {
            Assert.assertEquals(2, (int) brokerCount.getValue());
        }
    }

    @Test
    public void testDecommission() {
        String topic = "test";
        Map<Integer, List<Integer>> currentAssignment = ImmutableMap.of(
                0, (List<Integer>) ImmutableList.of(10, 11),
                1, ImmutableList.of(11, 12),
                2, ImmutableList.of(12, 13),
                3, ImmutableList.of(13, 10)
        );
        Set<Integer> newBrokers = ImmutableSet.of(10, 11, 13);
        KafkaTopicAssigner assigner = new KafkaTopicAssigner();
        Map<Integer, List<Integer>> newAssignment = assigner.generateAssignment(
                topic, currentAssignment, newBrokers, Collections.<Integer, String>emptyMap(), -1);

        Map<Integer, Integer> brokerReplicaCounts = verifyPartitionsAndBuildReplicaCounts(
                currentAssignment, newAssignment, 1);
        Assert.assertFalse(brokerReplicaCounts.containsKey(12));

        // Ensure that no broker is doing too much
        // In this case, there are 3 brokers, 4 partitions, and 2 replicas,
        // so one broker should be serving 2 replicas, and the others 3.
        int servingTwo = 0;
        int servingThree = 0;
        for (Map.Entry<Integer, Integer> brokerCount : brokerReplicaCounts.entrySet()) {
            switch (brokerCount.getValue()) {
                case 2:
                    servingTwo++;
                    break;
                case 3:
                    servingThree++;
                    break;
                default:
                    Assert.fail("No broker should serve fewer than two or greater than 3 replicas");
                    break;
            }
        }
        Assert.assertEquals(1, servingTwo);
        Assert.assertEquals(2, servingThree);
    }

    @Test
    public void testReplacement() {
        String topic = "test";
        Map<Integer, List<Integer>> currentAssignment = ImmutableMap.of(
                0, (List<Integer>) ImmutableList.of(10, 11),
                1, ImmutableList.of(11, 12),
                2, ImmutableList.of(12, 10),
                3, ImmutableList.of(10, 12)
        );
        Set<Integer> newBrokers = ImmutableSet.of(10, 11, 13);
        KafkaTopicAssigner assigner = new KafkaTopicAssigner();
        Map<Integer, List<Integer>> newAssignment = assigner.generateAssignment(
                topic, currentAssignment, newBrokers, Collections.<Integer, String>emptyMap(),-1);

        // run basic sanity checks
        Map<Integer, Integer> brokerReplicaCounts = verifyPartitionsAndBuildReplicaCounts(
                currentAssignment, newAssignment, 1);
        Assert.assertFalse(brokerReplicaCounts.containsKey(12));

        // there should have been no change here since broker 12 never served partition 0
        Assert.assertEquals(currentAssignment.get(0), newAssignment.get(0));

        // 11 should still be present in 1, and it can be joined by either 10 or 13
        Assert.assertTrue(newAssignment.get(1).contains(11));
        Assert.assertTrue(newAssignment.get(1).contains(10) || newAssignment.get(1).contains(13));

        // 10 should still be present in 2, and it can be joined by either 11 or 13
        Assert.assertTrue(newAssignment.get(2).contains(10));
        Assert.assertTrue(newAssignment.get(2).contains(11) || newAssignment.get(2).contains(13));

        // 10 should still be present in 3, and it can be joined by either 11 or 13
        Assert.assertTrue(newAssignment.get(3).contains(10));
        Assert.assertTrue(newAssignment.get(3).contains(11) || newAssignment.get(3).contains(13));
    }

    private Map<Integer, Integer> verifyPartitionsAndBuildReplicaCounts(
            Map<Integer, List<Integer>> currentAssignment,
            Map<Integer, List<Integer>> newAssignment, int minimalMovementThreshold) {
        Map<Integer, Integer> brokerReplicaCounts = Maps.newHashMap();
        for (Map.Entry<Integer, List<Integer>> partitionReplicas : newAssignment.entrySet()) {
            List<Integer> replicas = partitionReplicas.getValue();
            Set<Integer> replicaSet = ImmutableSet.copyOf(replicas);

            // Ensure that no broker is assigned twice to a replica
            Assert.assertEquals(replicas.size(), ImmutableSet.copyOf(replicas).size());

            // Keep track of how often a broker pops up
            for (Integer broker : replicas) {
                if (!brokerReplicaCounts.containsKey(broker)) {
                    brokerReplicaCounts.put(broker, 1);
                } else {
                    brokerReplicaCounts.put(broker, brokerReplicaCounts.get(broker) + 1);
                }
            }

            // Ensure that movement was minimal
            // Each partition should have brokers from the original assignment that "stuck"
            Set<Integer> prevReplicaSet = ImmutableSet.copyOf(
                    currentAssignment.get(partitionReplicas.getKey()));
            Assert.assertTrue(Sets.intersection(replicaSet, prevReplicaSet).size() >=
                    minimalMovementThreshold);
        }
        return brokerReplicaCounts;
    }
}
