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

            if (pair.getValue().topNeighbor == null ||
            pair.getValue().topNeighbor.getValue() < pair.getValue().pageRank) {
                dummyNode.topNeighbor = new Pair<>(pair.getKey(), pair.getValue().pageRank);
            }
            else {
                Pair<Long, Double> other = pair.getValue().topNeighbor;
                dummyNode.topNeighbor = new Pair<>(other.getKey(), other.getValue());
            }
            for(long childId : pair.getValue().adjacencyList) {
                keyWritable.set(childId);
                context.write(keyWritable, dummyNode);
            }
        }
    }

    static class NeighborhoodSearchReducer extends Reducer<LongWritable, GraphNode, LongWritable, GraphNode> {
        @Override
        protected void reduce(LongWritable key, Iterable<GraphNode> values, Context context) throws IOException, InterruptedException {
            GraphNode node = null;
            Pair<Long, Double> localTopNeighbor = null;

            for (GraphNode value : values) {
                if (value.nodeId == -1) { // Dummy Node
                    if (localTopNeighbor == null ||
                    localTopNeighbor.getValue() < value.topNeighbor.getValue()) {
                        localTopNeighbor = new Pair<>(value.topNeighbor.getKey(), value.topNeighbor.getValue());
                    }
                }
                else {
                    node = new GraphNode(value);
                }
            }

            if (node != null) {
                node.topNeighbor = localTopNeighbor;
                context.write(key, node);
            }
        }
    }
}
