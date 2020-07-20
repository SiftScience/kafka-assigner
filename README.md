kafka-assigner
==============

[![CircleCI](https://circleci.com/gh/SiftScience/kafka-assigner.svg?style=svg)](https://circleci.com/gh/SiftScience/kafka-assigner)

This is a rack-aware tool for assigning Kafka partitions to brokers that minimizes data movement. It also includes the ability to inspect the current live brokers in the cluster and the current partition assignment.

**Using this tool will greatly simplify operations like decommissioning a broker, adding a new broker, or replacing a broker.**

# Why is this necessary?
Kafka's built-in algorithm is easy to use and monitor, but it does not take into account existing assignments of partitions to nodes. Instead, the burden is on the operator to either move entire topics across brokers, or come up with a sane way of moving some number of partitions of existing topics. This is extremely disruptive.

This tool _minimizes_ the number of partitions already assigned that need to leave a given node, while ensuring that each broker is responsible for a similar number of partitions. This enables use cases like node replacement, in which we would like to bring up a broker that is responsible for the same data as a misbehaving broker that it is replacing.

# How does this work?
This tool uses a strategy that behaves similarly to [Apache Helix](http://helix.apache.org)'s auto-rebalancing algorithm. It first assigns as many already-assigned partitions back to nodes as it can (while ensuring that no node is overloaded), and then evenly assigns all other partitions such that every node eventually ends up responsible for roughly the same number of partitions.

# How is this tool used?

## Get the tool
1. Download from the "Releases" page
2. `tar xf kafka-assigner-1.1-pkg.tar`
3. `cd kafka-assigner-1.1/bin`

## Run the tool
Requires Java 1.7+

```
./kafka-assignment-generator.sh [options...] arguments...
 --bootstrap_server VAL                 : Kafka Broker(s) to bootstrap
                                          connection (comma-separated host:port
                                          pairs)
 --broker_hosts VAL                     : comma-separated list of broker
                                          hostnames (instead of broker IDs)
 --broker_hosts_to_remove VAL           : comma-separated list of broker
                                          hostnames to exclude (instead of
                                          broker IDs)
 --disable_rack_awareness               : set to true to ignore rack
                                          configurations
 --integer_broker_ids VAL               : comma-separated list of Kafka broker
                                          IDs (integers)
 --mode [PRINT_CURRENT_ASSIGNMENT |     : the mode to run (PRINT_CURRENT_ASSIGNM
 PRINT_CURRENT_BROKERS |                  ENT, PRINT_CURRENT_BROKERS,
 PRINT_REASSIGNMENT]                      PRINT_REASSIGNMENT)
 --topics VAL                           : comma-separated list of topics
```

### Example: reassign partitions to all live hosts
```
./kafka-assignment-generator.sh --bootstrap_server my-kafka-host:9092 --mode PRINT_REASSIGNMENT
```

The output JSON can then be fed into Kafka's reassign partitions command. See [here](http://kafka.apache.org/0100/ops.html#basic_ops_partitionassignment) for instructions.

### Example: reassign partitions to all but a few live hosts
This mode is useful for decommissioning or replacing a node. The partitions will be assigned to all live hosts, excluding the hosts that are specified.
```
./kafka-assignment-generator.sh --bootstrap_server my-kafka-host:9092 --mode PRINT_REASSIGNMENT --broker_hosts_to_remove misbehaving-host1,misbehaving-host2
```

The output JSON can then be fed into Kafka's reassign partitions command. See [here](http://kafka.apache.org/0100/ops.html#basic_ops_partitionassignment) for instructions.

### Example: reassign partitions to specific hosts
Note that in this mode, it is expected that every host that should own partitions should be specified, including existing ones.
```
./kafka-assignment-generator.sh --bootstrap_server my-kafka-host:9092 --mode PRINT_REASSIGNMENT --broker_hosts host1,host2,host3
```

The output JSON can then be fed into Kafka's reassign partitions command. See [here](http://kafka.apache.org/0100/ops.html#basic_ops_partitionassignment) for instructions.

### Example: print current brokers
```
./kafka-assignment-generator.sh --bootstrap_server my-kafka-host:9092 --mode PRINT_CURRENT_BROKERS
```

### Example: print current assignment
```
./kafka-assignment-generator.sh --bootstrap_server my-kafka-host:9092 --mode PRINT_CURRENT_ASSIGNMENT
```

# Building
Requires Java 1.7+ and Maven 3.2+

1. Clone this repository
2. `mvn install package`
3. Artifacts are in `target/kafka-assigner-pkg`

# License
Licensed under the Apache License 2.0.
