package sh.oso.connect.ai.adapter.kafka;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class PartitionAssignerTest {

    @Test
    void assignsEvenlyToTwoTasks() {
        Set<TopicPartition> partitions = Set.of(
                new TopicPartition("topic-a", 0),
                new TopicPartition("topic-a", 1),
                new TopicPartition("topic-a", 2),
                new TopicPartition("topic-a", 3)
        );

        List<List<TopicPartition>> assignments = PartitionAssigner.assign(partitions, 2);
        assertEquals(2, assignments.size());
        assertEquals(2, assignments.get(0).size());
        assertEquals(2, assignments.get(1).size());
    }

    @Test
    void assignsUnevenlyWhenNotDivisible() {
        Set<TopicPartition> partitions = Set.of(
                new TopicPartition("topic-a", 0),
                new TopicPartition("topic-a", 1),
                new TopicPartition("topic-a", 2)
        );

        List<List<TopicPartition>> assignments = PartitionAssigner.assign(partitions, 2);
        assertEquals(2, assignments.size());
        assertEquals(2, assignments.get(0).size());
        assertEquals(1, assignments.get(1).size());
    }

    @Test
    void assignsAllPartitionsToSingleTask() {
        Set<TopicPartition> partitions = Set.of(
                new TopicPartition("t", 0),
                new TopicPartition("t", 1)
        );

        List<List<TopicPartition>> assignments = PartitionAssigner.assign(partitions, 1);
        assertEquals(1, assignments.size());
        assertEquals(2, assignments.get(0).size());
    }

    @Test
    void handleMoreTasksThanPartitions() {
        Set<TopicPartition> partitions = Set.of(
                new TopicPartition("t", 0)
        );

        List<List<TopicPartition>> assignments = PartitionAssigner.assign(partitions, 3);
        assertEquals(3, assignments.size());
        assertEquals(1, assignments.get(0).size());
        assertEquals(0, assignments.get(1).size());
        assertEquals(0, assignments.get(2).size());
    }

    @Test
    void multipleTopicsDistributedByRoundRobin() {
        Set<TopicPartition> partitions = Set.of(
                new TopicPartition("alpha", 0),
                new TopicPartition("alpha", 1),
                new TopicPartition("beta", 0),
                new TopicPartition("beta", 1)
        );

        List<List<TopicPartition>> assignments = PartitionAssigner.assign(partitions, 2);
        assertEquals(2, assignments.size());
        // Each task should get 2 partitions total
        assertEquals(2, assignments.get(0).size());
        assertEquals(2, assignments.get(1).size());
    }
}
