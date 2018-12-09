package wc;

import javafx.util.Pair;

import java.io.IOException;

public class Util {

    static Pair<Long, GraphNode> parseGraphNodeFromRecordCSV(String record) throws IOException {
        int firstComma = record.indexOf(',');
        String part1 = record.substring(0, firstComma);
        String part2 = record.substring(firstComma + 1);

        return new Pair<Long, GraphNode>(Long.parseLong(part1), GraphNode.parseJson(part2));
    }
}
