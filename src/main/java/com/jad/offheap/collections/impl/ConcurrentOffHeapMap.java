package com.jad.offheap.collections.impl;

import com.jad.offheap.collections.serializer.Serializer;

import java.util.*;
import java.util.concurrent.Semaphore;

/**
 * @author: Ilya Krokhmalyov YC14IK1
 * @since: 11/10/13
 */

public class ConcurrentOffHeapMap<K, V> extends AbstractMap<K, V> {

    /**
     * The default initial capacity for this table,
     * used when not otherwise specified in a constructor.
     */
    static final int DEFAULT_INITIAL_CAPACITY = 16;

    /**
     * The default load factor for this table, used when not
     * otherwise specified in a constructor.
     */
    static final float DEFAULT_LOAD_FACTOR = 0.75f;

    /**
     * The default concurrency level for this table, used when not
     * otherwise specified in a constructor.
     */
    static final int DEFAULT_CONCURRENCY_LEVEL = 16;

    /**
     * The maximum capacity, used if a higher value is implicitly
     * specified by either of the constructors with arguments.  MUST
     * be a power of two <= 1<<30 to ensure that entries are indexable
     * using ints.
     */
    static final int MAXIMUM_CAPACITY = 1 << 30;

    /**
     * The maximum number of segments to allow; used to bound
     * constructor arguments.
     */
    static final int MAX_SEGMENTS = 1 << 16; // slightly conservative

    /**
     * Number of unsynchronized retries in size and containsValue
     * methods before resorting to locking. This is used to avoid
     * unbounded retries if tables undergo continuous modification
     * which would make it impossible to obtain an accurate result.
     */
    static final int RETRIES_BEFORE_LOCK = 2;

    final int segmentMask;

    /**                       getKvEntry
     * Shift value for indexing within segments.
     */
    final int segmentShift;

    private Segment<K, V>[] segments;

    public static final Serializer<byte[]> STUB = new Serializer<byte[]>() {
        @Override
        public byte[] serialize(byte[] key) {
            return key;
        }

        @Override
        public byte[] deserialize(byte[] bytes) {
            return bytes;
        }
    };

    private static class Segment<N, P> extends Semaphore {
        private static final int MAX_PERMITS = 1024;


        private OffHeapHashMap<N, P> map;

        public Segment(int initCap, float loadf, Serializer<N> keyS, Serializer<P> valSer) {
            super(MAX_PERMITS, true);
            map = new OffHeapHashMap<N, P>(initCap, loadf, keyS, valSer);
        }

        public P get(Object key, int hash) {
            acquireUninterruptibly();
            try {
                return map.get(key, hash);
            } finally {
                release();
            }
        }

        public P put(N key, P val, int hash) {
            acquireUninterruptibly(MAX_PERMITS);
            try {
                return map.put(key, val, hash);
            } finally {
                release(MAX_PERMITS);
            }
        }

        public boolean contains(Object key, int hash) {
            acquireUninterruptibly();
            try {
                return map.getEntry(key, hash) > 0;
            } finally {
                release();
            }
        }

        public void clear() {
            map.clear();
        }

        public P remove(Object key, int hash) {
            acquireUninterruptibly(MAX_PERMITS);
            try {
                return map.remove(key, hash);
            } finally {
                release(MAX_PERMITS);
            }
        }

        public Set<N> keySet() {
            return map.keySet();
        }

        private class HashIterator implements Iterator<Entry<N, P>> {
            private int tablePosition = 0;
            private int currentNum = 0;
            private Entry<N,P> current = null;
            private Entry<N,P> next = null;

            @Override
            public boolean hasNext() {
                if (next != null) {
                    return true;
                }
                acquireUninterruptibly();
                try {
                    while (tablePosition < map.tableLength) {
                        if ((next = map.getEntryAtPosition(tablePosition, currentNum++)) != null ) {
                            return true;
                        } else {
                            tablePosition++;
                            currentNum = 0;
                        }
                    }
                } finally {
                    release();
                }
                return false;
            }

            @Override
            public Entry<N, P> next() {
                if (hasNext()) {
                    current = next;
                    next = null;
                    return current;
                }
                throw new NoSuchElementException();
            }

            @Override
            public void remove() {
                if (current != null) {
                    N key = current.getKey();   //TODO without deserialize;
                    map.remove(key, hash(key.hashCode()) );
                    currentNum--;
                } else {
                    throw new IllegalStateException();
                }
            }
        }

        public Iterator<Entry<N, P>> entryIterator() {
            return new HashIterator();
        }

        public int size() {
            acquireUninterruptibly();
            try {
                return map.size();
            } finally {
                release();
            }
        }
    }

    private static int hash(int h) {
        // Spread bits to regularize both segment and index locations,
        // using variant of single-word Wang/Jenkins hash.
        h += (h <<  15) ^ 0xffffcd7d;
        h ^= (h >>> 10);
        h += (h <<   3);
        h ^= (h >>>  6);
        h += (h <<   2) + (h << 14);
        return h ^ (h >>> 16);
    }

    final Segment<K,V> segmentFor(int hash) {
        return segments[(hash >>> segmentShift) & segmentMask];
    }

    public ConcurrentOffHeapMap(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR, DEFAULT_CONCURRENCY_LEVEL, keySerializer, valueSerializer);
    }

    public ConcurrentOffHeapMap(int initialCapacity,
                                float loadFactor, int concurrencyLevel, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        if (!(loadFactor > 0) || initialCapacity < 0 || concurrencyLevel <= 0 || keySerializer == null || valueSerializer == null)
            throw new IllegalArgumentException();

        if (concurrencyLevel > MAX_SEGMENTS)
            concurrencyLevel = MAX_SEGMENTS;

        // Find power-of-two sizes best matching arguments
        int sshift = 0;
        int ssize = 1;
        while (ssize < concurrencyLevel) {
            ++sshift;
            ssize <<= 1;
        }
        segmentShift = 32 - sshift;
        segmentMask = ssize - 1;
        segments = new Segment[ssize];
        //this.segments = Segment.newArray(ssize);

        if (initialCapacity > MAXIMUM_CAPACITY)
            initialCapacity = MAXIMUM_CAPACITY;
        int c = initialCapacity / ssize;
        if (c * ssize < initialCapacity)
            ++c;
        int cap = 1;
        while (cap < c)
            cap <<= 1;

        for (int i = 0; i < this.segments.length; ++i)
            this.segments[i] = new Segment<K,V>(cap, loadFactor, keySerializer, valueSerializer);
    }

    public V get(Object key) {
        int hash = hash(key.hashCode());
        return segmentFor(hash).get(key, hash);
    }

    public V put(K key, V value) {
        int hash = hash(key.hashCode());
        return segmentFor(hash).put(key, value, hash);
    }

    @Override
    public boolean containsKey(Object key) {
        int hash = hash(key.hashCode());
        return segmentFor(hash).contains(key, hash);
    }

    @Override
    public void clear() {
        for (Segment<K, V> segment : segments) {
            segment.clear();
        }
        segments = null;
    }

    @Override
    public V remove(Object key) {
        int hash = hash(key.hashCode());
        return segmentFor(hash).remove(key, hash);
    }

    private class EntrySet extends AbstractSet<Entry<K, V>> {

        @Override
        public Iterator<Entry<K, V>> iterator() {
            return new Iterator<Entry<K, V>>() {
                private int currentSegment = -1;
                private Iterator<Entry<K,V>> currentSegmentIterator;
                private Entry<K, V> current;
                private Entry<K, V> next;

                @Override
                public boolean hasNext() {
                    if (next != null) {
                        return true;
                    }
                    if (currentSegmentIterator != null && currentSegmentIterator.hasNext()) {
                        next = currentSegmentIterator.next();
                        return true;
                    }
                    while (++currentSegment < segments.length) {
                        currentSegmentIterator = segments[currentSegment].entryIterator();
                        if (currentSegmentIterator != null && currentSegmentIterator.hasNext()) {
                            next = currentSegmentIterator.next();
                            return true;
                        }
                    }
                    return false;
                }

                @Override
                public Entry<K, V> next() {
                    if (hasNext()) {
                        current = next;
                        next = null;
                        return current;
                    } else {
                        throw new  NoSuchElementException();
                    }
                }

                @Override
                public void remove() {
                    if (current != null) {
                        currentSegmentIterator.remove();
                    }
                }
            };
        }

        @Override
        public int size() {
            return ConcurrentOffHeapMap.this.size();
        }

        @Override
        public void clear() {
            ConcurrentOffHeapMap.this.clear();
        }

        @Override
        public boolean contains(Object o) {
            if (!(o instanceof Entry)) {
                return false;
            }
            Entry obj = (Entry) o;
            V v = get(obj.getKey());
            Object v1 = ((Entry) o).getValue();
            if (v == null) {
                return v1 == null;
            } else {
                return v == v1 || v.equals(v1);
            }
        }

        @Override
        public boolean remove(Object o) {
            if (!(o instanceof Entry) ) {
                return false;
            }
            return ConcurrentOffHeapMap.this.remove(((Entry) o).getKey()) != null;
        }
    }

    private class KeySet extends AbstractSet<K> {
        @Override
        public Iterator<K> iterator() {
            return new Iterator<K>() {
                private Iterator<Entry<K, V>> entrySetIterator = new EntrySet().iterator();
                @Override
                public boolean hasNext() {
                    return entrySetIterator.hasNext();
                }

                @Override
                public K next() {
                    return entrySetIterator.next().getKey();
                }

                @Override
                public void remove() {
                    entrySetIterator.remove();
                }
            };
        }

        @Override
        public boolean contains(Object o) {
            return ConcurrentOffHeapMap.this.containsKey(o);
        }

        @Override
        public void clear() {
            ConcurrentOffHeapMap.this.clear();
        }

        @Override
        public boolean remove(Object o) {
            return ConcurrentOffHeapMap.this.remove(o) != null;
        }

        @Override
        public int size() {
            return ConcurrentOffHeapMap.this.size();
        }
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return new EntrySet();
    }

    @Override
    public Set<K> keySet() {
        return new KeySet();
    }

    @Override
    public int size() {
        int sum = 0;
        for (Segment<K, V> segment : segments) {
            sum += segment.size();
        }
        return sum;

    }
}
