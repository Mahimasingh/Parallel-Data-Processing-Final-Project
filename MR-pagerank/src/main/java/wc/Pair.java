package wc;

public class Pair<K, V> implements Comparable {

    private K key;
    private V value;

    K getKey() {
        return key;
    }

    V getValue() {
        return value;
    }

    Pair(K okey, V ovalue) {
        this.key = okey;
        this.value = ovalue;
    }

    @Override
    public int compareTo(Object o) {
        return 0;
    }
}
