package wc;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

/**
 * The purpose of this class is to use the given input file with individual <follower user>
 * relationships into an adjacency list with the format <follower user1 user2 ... user n>
 */

public class AdjacencyListJob {

    public static class AdjacencyListMapper extends Mapper<Object, Text, LongWritable, LongWritable> {
        private static LongWritable fromEdge = new LongWritable();
        private static LongWritable toEdges = new LongWritable();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String edge = value.toString();
            String[] tokens = edge.split("\\s+");

            if (tokens.length == 2) {
                long follower = Long.parseLong(tokens[1]); //swapping the follower and followed
                long followed = Long.parseLong(tokens[0]);

                fromEdge.set(follower);
                toEdges.set(followed);

                context.write(fromEdge, toEdges);
                fromEdge.set(-1);
                context.write(toEdges, fromEdge);
            }
        }
    }

    public static class AdjacencyListReducer extends Reducer<LongWritable, LongWritable, LongWritable, GraphNode> {
        @Override
        protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            List<Long> following = new ArrayList<>();
            for (LongWritable val : values) {
                if (val.get() != -1)
                    following.add(val.get());
            }

            GraphNode node = new GraphNode(key.get(), following);
            context.write(key, node);
        }
    }
}
