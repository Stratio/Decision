/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.streaming.utils;

import com.stratio.streaming.exception.CacheException;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Storage of objects by key.
 */
public interface Cache<K, V> {

    public V get(K key) throws CacheException;

    public V put(K key, V value) throws CacheException;

    public void putAll(Map<K, V> elements) throws CacheException;

    public V remove(K key) throws CacheException;

    public void clear() throws CacheException;

    public int size();

    public Set<K> keys();

    public Collection<V> values();

    public Map<K, V> asMap();
}
