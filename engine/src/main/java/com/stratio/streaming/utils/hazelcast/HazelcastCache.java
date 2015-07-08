package com.stratio.streaming.utils.hazelcast;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.stratio.streaming.exception.CacheException;
import com.stratio.streaming.utils.Cache;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.context.annotation.Configuration;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Cache implementation wrapping a Hazelcast distributed instance.
 */
@Configuration
public class HazelcastCache<K, V> implements Cache<K, V> {

    private IMap<K, V> iMap;

    public HazelcastCache(HazelcastInstance hazelcastInstance, String name) {
        this.iMap = hazelcastInstance.getMap(name);
    }

    @Override
    public V get(K key) throws CacheException {
        return iMap.get(key);
    }

    @Override
    public V put(K key, V value) throws CacheException {
        return iMap.put(key, value);
    }

    @Override
    public void putAll(Map<K, V> elements) throws CacheException {
        iMap.putAll(elements);
    }

    @Override
    public V remove(K key) throws CacheException {
        return iMap.remove(key);
    }

    @Override
    public void clear() throws CacheException {
        iMap.clear();
    }

    @Override
    public int size() {
        return iMap.size();
    }

    @Override
    public Set<K> keys() {
        Set<K> keys = iMap.keySet();
        if (!CollectionUtils.isEmpty(keys)) {
            return Collections.unmodifiableSet(keys);
        }
        return Collections.emptySet();
    }

    @Override
    public Collection<V> values() {
        Collection<V> values = iMap.values();
        if (!CollectionUtils.isEmpty(values)) {
            return Collections.unmodifiableCollection(values);
        }
        return Collections.emptySet();
    }

    @Override
    public Map<K, V> asMap() {
        return this.iMap;
    }
}
