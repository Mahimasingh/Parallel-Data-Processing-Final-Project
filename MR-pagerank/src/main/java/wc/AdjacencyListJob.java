package wc;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringJoiner;

/**
 * The purpose of this class is to use the given input file with individual <follower user>
 * relationships into an adjacency list with the format <follower user1 user2 ... user n>
 */

public class AdjacencyListJob {

    public static class AdjacencyListMapper extends Mapper<Object, Text, LongWritable, Text> {
        private static LongWritable fromEdge = new LongWritable();
        private static Text toEdges = new Text();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String edge = value.toString();
            String[] tokens = edge.split("\\s+");

            if (tokens.length == 2) {
                long follower = Long.parseLong(tokens[1]); //swapping the follower and followed
                String followed = tokens[0];

                fromEdge.set(follower);
                toEdges.set(followed);

                context.write(fromEdge, toEdges);
            }
        }
    }

    public static class AdjacencyListReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Text adjacency = new Text();
            StringJoiner joiner = new StringJoiner(",");
            for (Text val : values) {
                joiner.add(val.toString());
            }
            adjacency.set(joiner.toString());
            context.write(key, adjacency);
        }
    }
}
