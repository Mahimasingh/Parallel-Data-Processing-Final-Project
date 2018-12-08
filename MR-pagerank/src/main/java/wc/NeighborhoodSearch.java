package wc;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NeighborhoodSearch {
    static class NeighborhoodSearchMapper extends Mapper<Object, Text, LongWritable, GraphNode> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String nodeText = value.toString();
            if(nodeText.contains("{")){
                // This is adjacency List

            }

        }
    }

    static class NeighborhoodSearchReducer extends Reducer<LongWritable, GraphNode, LongWritable, GraphNode> {
        @Override
        protected void reduce(LongWritable key, Iterable<GraphNode> values, Context context) throws IOException, InterruptedException {
            super.reduce(key, values, context);
        }
    }
}
