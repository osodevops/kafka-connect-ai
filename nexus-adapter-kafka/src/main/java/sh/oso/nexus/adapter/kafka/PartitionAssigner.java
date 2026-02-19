package sh.oso.nexus.adapter.kafka;

import org.apache.kafka.common.TopicPartition;

import java.util.*;

public class PartitionAssigner {

    public static List<List<TopicPartition>> assign(Collection<TopicPartition> partitions, int numTasks) {
        List<TopicPartition> sortedPartitions = new ArrayList<>(partitions);
        sortedPartitions.sort(Comparator
                .comparing(TopicPartition::topic)
                .thenComparingInt(TopicPartition::partition));

        List<List<TopicPartition>> assignments = new ArrayList<>();
        for (int i = 0; i < numTasks; i++) {
            assignments.add(new ArrayList<>());
        }

        for (int i = 0; i < sortedPartitions.size(); i++) {
            assignments.get(i % numTasks).add(sortedPartitions.get(i));
        }

        return assignments;
    }
}
