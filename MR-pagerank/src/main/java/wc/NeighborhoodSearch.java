package wc;

import javafx.util.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.PriorityQueue;

public class NeighborhoodSearch {
    static class NeighborhoodSearchMapper extends Mapper<Object, Text, LongWritable, GraphNode> {
        private static LongWritable keyWritable = new LongWritable();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Pair<Long, GraphNode> pair = Util.parseGraphNodeFromRecordCSV(value.toString());
            keyWritable.set(pair.getKey());
            context.write(keyWritable, pair.getValue());

            GraphNode dummyNode = new GraphNode();
            dummyNode.nodeId = -1;
            dummyNode.pageRank = pair.getValue().pageRank;
            for(long childId : pair.getValue().adjacencyList) {
                keyWritable.set(childId);
                context.write(keyWritable, dummyNode);
            }
        }
    }

    static class NeighborhoodSearchReducer extends Reducer<LongWritable, GraphNode, LongWritable, GraphNode> {
        private static PriorityQueue<Double> priorityQueue = new PriorityQueue<Double>(5);
        @Override
        protected void reduce(LongWritable key, Iterable<GraphNode> values, Context context) throws IOException, InterruptedException {
            priorityQueue.clear();

            GraphNode node = null;
            for (GraphNode value : values) {
                if (value.nodeId == -1) { // Dummy Node
                    if (priorityQueue.isEmpty() ||
                            (priorityQueue.size() < 5) ||
                            (priorityQueue.size() == 5 && priorityQueue.peek() < value.pageRank)) {
                        if (priorityQueue.size() == 5)
                            priorityQueue.poll();

                        priorityQueue.add(value.pageRank);
                    }
                }
                else {
                    node = new GraphNode(value);
                }
            }

            if (node != null) {
                long i = 1;
                while (!priorityQueue.isEmpty()) {
                    node.distanceMap.put(i, priorityQueue.poll());
                    i += 1;
                }
                context.write(key, node);
            }
        }
    }
}
