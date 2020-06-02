package ch.derlin.bbdata.flink.utils;

import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * date: 29.05.20
 *
 * @author Lucy Linder <lucy.derlin@gmail.com>
 */
public class MockCollector<T> implements Collector<T> {
    private List<T> _collected = new ArrayList<>();

    @Override
    public void collect(T item) {
        _collected.add(item);
    }

    @Override
    public void close() {
    }

    public List<T> getAll() {
        return _collected;
    }

    public T get(int index) {
        return _collected.get(index);
    }

    public T getLast() {
        if (_collected.size() > 0) return _collected.get(_collected.size() - 1);
        return null;
    }

    public int size() {
        return _collected.size();
    }

}