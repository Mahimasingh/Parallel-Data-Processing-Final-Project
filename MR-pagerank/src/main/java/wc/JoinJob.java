package wc;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class JoinJob {
    static class JoinJobMapper extends Mapper<Object, Text, LongWritable, GraphNode> {
        private static LongWritable keyWritable = new LongWritable();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String valueString = value.toString();
            if (valueString.length() > 0 && valueString.charAt(0) == '(') {
                valueString = valueString.replaceFirst("\\(", "");
                valueString = valueString.replaceFirst("\\)", "");

                String[] parts = valueString.split(",");
                Long nodeId = Long.parseLong(parts[0]);
                Double pageRank = Double.parseDouble(parts[1]);

                GraphNode node = new GraphNode();
                node.nodeId = new Long(-1); // mark dummy
                node.pageRank = pageRank;

                keyWritable.set(nodeId);
                context.write(keyWritable, node);

            }
            else {
                int firstComma = valueString.indexOf(',');
                String part1 = valueString.substring(0, firstComma);
                String part2 = valueString.substring(firstComma + 1);

                Long nodeId = Long.parseLong(part1);
                GraphNode node = GraphNode.parseJson(part2);

                keyWritable.set(nodeId);
                context.write(keyWritable, node);
            }
        }
    }

    static class JoinJobReducer extends Reducer<LongWritable, GraphNode, LongWritable, GraphNode> {
        @Override
        protected void reduce(LongWritable key, Iterable<GraphNode> values, Context context) throws IOException, InterruptedException {
            GraphNode node = null;
            Double pageRank = 0.0;
            for(GraphNode value : values) {
                if (value.nodeId == -1) { // here we have received the page rank
                    pageRank = value.pageRank;
                }
                else { // Here we are getting a graph
                    node = new GraphNode(value);
                }
            }
            if (node != null) {
                node.pageRank = pageRank;
                context.write(key, node);
            }
        }
    }
}
