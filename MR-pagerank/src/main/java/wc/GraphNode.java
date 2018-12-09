package wc;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;

public class GraphNode implements WritableComparable<GraphNode> {

    @SerializedName("nid")
    long nodeId;

    @SerializedName("pr")
    Double pageRank;

    @SerializedName("top")
    Pair<Long, Double> topNeighbor;

    @SerializedName("adj")
    List<Long> adjacencyList;

    public GraphNode() {
        adjacencyList = new ArrayList<>();
    }

    public GraphNode(Long nodeId, List<Long> adjacencyList) {
        this.nodeId = nodeId;
        this.topNeighbor = null;
        this.pageRank = 0.0;
        this.adjacencyList = adjacencyList;
    }


    GraphNode(GraphNode other) {
        this.nodeId = other.nodeId;
        this.topNeighbor = other.topNeighbor;
        this.adjacencyList = other.adjacencyList;
        this.pageRank = other.pageRank;
    }

    private String stringRepresentation () throws IOException {
        Gson gson = new Gson();

        return gson.toJson(this);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeString(dataOutput, stringRepresentation());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        String json = WritableUtils.readString(dataInput);
        Gson gson = new Gson();

        GraphNode other = gson.fromJson(json, GraphNode.class);

        this.nodeId = other.nodeId;
        this.adjacencyList = other.adjacencyList;
        this.topNeighbor = other.topNeighbor;
        this.pageRank = other.pageRank;
    }

    public static GraphNode parseJson(String json) throws IOException {
        Gson gson = new Gson();

        GraphNode other = gson.fromJson(json, GraphNode.class);

        return other;
    }

//    public static GraphNode parseRaw(String s) throws IOException {
//        String[] components = s.split(",");
//        GraphNode node = new GraphNode();
//
//        node.nodeId = Long.parseLong(components[0]);
//        node.distanceMap = new HashMap<>();
//        node.adjacencyList = new ArrayList<>();
//
//        for (int i = 1; i < components.length; i++) {
//            node.adjacencyList.add(Integer.parseInt(components[i]));
//        }
//
//        return node;
//    }

    @Override
    public String toString() {
        try {
            return stringRepresentation();
        }
        catch (IOException ex) {}
        return super.toString();
    }

    @Override
    public int compareTo(GraphNode o) {
        return 0;
    }
}