package com.jad.offheap.collections.impl;


import com.jad.offheap.UnsafeWrapper;
import com.jad.offheap.collections.serializer.Serializer;

import java.io.IOException;
import java.util.*;


/**
 * @author: Ilya Krokhmalyov jad7kii@gmail.com
 * @since: 11/14/13
 */

public class OffHeapHashMap<K, V> extends AbstractMap<K,V>
        implements Map<K,V>, Cloneable {
    private static UnsafeWrapper unsafe = UnsafeWrapper.unsafeWrapper;
    private static final int BYTE_ARRAY_OFFSET = unsafe.arrayBaseOffset(byte[].class);
    private static final byte[] NULL_ARR = new byte[]{};
    
    /**
     * The default initial capacity - MUST be a power of two.
     */
    static final int DEFAULT_INITIAL_CAPACITY = 16;

    /**
     * The maximum capacity, used if a higher value is implicitly specified
     * by either of the constructors with arguments.
     * MUST be a power of two <= 1<<30.
     */
    static final int MAXIMUM_CAPACITY = 1 << 30;

    /**
     * The load factor used when none specified in constructor.
     */
    static final float DEFAULT_LOAD_FACTOR = 0.75f;

    /**
     * The table, resized as necessary. Length MUST Always be a power of two.
     */
    transient long table;

    transient int tableLength;

    /**
     * The number of key-value mappings contained in this map.
     */
    transient int size;

    /**
     * The next size value at which to resize (capacity * load factor).
     * @serial
     */
    int threshold;

    private Serializer<K> keySerializer;
    private Serializer<V> valueSerializer;


    /**
     * The load factor for the hash table.
     *
     * @serial
     */
    final float loadFactor;

    /**
     * The number of times this HashMap has been structurally modified
     * Structural modifications are those that change the number of mappings in
     * the HashMap or otherwise modify its internal structure (e.g.,
     * rehash).  This field is used to make iterators on Collection-views of
     * the HashMap fail-fast.  (See ConcurrentModificationException).
     */
    transient volatile int modCount;

    /**
     * Constructs an empty <tt>HashMap</tt> with the specified initial
     * capacity and load factor.
     *
     * @param  initialCapacity the initial capacity
     * @param  loadFactor      the load factor
     * @throws IllegalArgumentException if the initial capacity is negative
     *         or the load factor is nonpositive
     */
    public OffHeapHashMap(int initialCapacity, float loadFactor, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        if (initialCapacity < 0)
            throw new IllegalArgumentException("Illegal initial capacity: " +
                    initialCapacity);
        if (initialCapacity > MAXIMUM_CAPACITY)
            initialCapacity = MAXIMUM_CAPACITY;
        if (loadFactor <= 0 || Float.isNaN(loadFactor))
            throw new IllegalArgumentException("Illegal load factor: " +
                    loadFactor);

        // Find a power of 2 >= initialCapacity
        int capacity = 1;
        while (capacity < initialCapacity)
            capacity <<= 1;

        this.loadFactor = loadFactor;
        threshold = (int)(capacity * loadFactor);

        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;

        table = newTable(capacity);
        init();
    }

    private long newTable(int capacity) {
        tableLength = capacity;
        long l = unsafe.allocateMemory(capacity << 3);
        if (l <= 0) {
            throw new OutOfMemoryError();
        }
        unsafe.setMemory(l, ((long)capacity) << 3, (byte)0);
        return l;
    }

    /**
     * Constructs an empty <tt>HashMap</tt> with the specified initial
     * capacity and the default load factor (0.75).
     *
     * @param  initialCapacity the initial capacity.
     * @throws IllegalArgumentException if the initial capacity is negative.
     */
    public OffHeapHashMap(int initialCapacity, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this(initialCapacity, DEFAULT_LOAD_FACTOR, keySerializer, valueSerializer);
    }

    /**
     * Constructs an empty <tt>HashMap</tt> with the default initial capacity
     * (16) and the default load factor (0.75).
     */
    public OffHeapHashMap(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.loadFactor = DEFAULT_LOAD_FACTOR;
        threshold = (int)(DEFAULT_INITIAL_CAPACITY * DEFAULT_LOAD_FACTOR);
        table = newTable(DEFAULT_INITIAL_CAPACITY);
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        init();
    }

    /**
     * Constructs a new <tt>HashMap</tt> with the same mappings as the
     * specified <tt>Map</tt>.  The <tt>HashMap</tt> is created with
     * default load factor (0.75) and an initial capacity sufficient to
     * hold the mappings in the specified <tt>Map</tt>.
     *
     * @param   m the map whose mappings are to be placed in this map
     * @throws NullPointerException if the specified map is null
     */
    public OffHeapHashMap(Map<? extends K, ? extends V> m,Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this(Math.max((int) (m.size() / DEFAULT_LOAD_FACTOR) + 1,
                DEFAULT_INITIAL_CAPACITY), DEFAULT_LOAD_FACTOR, keySerializer, valueSerializer);
        putAllForCreate(m);
    }

    // internal utilities

    /**
     * Initialization hook for subclasses. This method is called
     * in all constructors and pseudo-constructors (clone, readObject)
     * after HashMap has been initialized but before any entries have
     * been inserted.  (In the absence of this method, readObject would
     * require explicit knowledge of subclasses.)
     */
    void init() {
    }

    /**
     * Applies a supplemental hash function to a given hashCode, which
     * defends against poor quality hash functions.  This is critical
     * because HashMap uses power-of-two length hash tables, that
     * otherwise encounter collisions for hashCodes that do not differ
     * in lower bits. Note: Null keys always map to hash 0, thus index 0.
     */
    protected int hash(int h) {
        // This function ensures that hashCodes that differ only by
        // constant multiples at each bit position have a bounded
        // number of collisions (approximately 8 at default load factor).
        h ^= (h >>> 20) ^ (h >>> 12);
        return h ^ (h >>> 7) ^ (h >>> 4);
        //return Hashing.murmur3_32().hashInt(h).asInt();
    }


    /**
     * Returns index for hash code h.
     */
    protected int indexFor(int h, int length) {
        return h & (length-1);
    }

    /**
     * Returns the number of key-value mappings in this map.
     *
     * @return the number of key-value mappings in this map
     */
    public int size() {
        return size;
    }

    /**
     * Returns <tt>true</tt> if this map contains no key-value mappings.
     *
     * @return <tt>true</tt> if this map contains no key-value mappings
     */
    public boolean isEmpty() {
        return size == 0;
    }

    /**
     * Returns the value to which the specified key is mapped,
     * or {@code null} if this map contains no mapping for the key.
     *
     * <p>More formally, if this map contains a mapping from a key
     * {@code k} to a value {@code v} such that {@code (key==null ? k==null :
     * key.equals(k))}, then this method returns {@code v}; otherwise
     * it returns {@code null}.  (There can be at most one such mapping.)
     *
     * <p>A return value of {@code null} does not <i>necessarily</i>
     * indicate that the map contains no mapping for the key; it's also
     * possible that the map explicitly maps the key to {@code null}.
     * The {@link #containsKey containsKey} operation may be used to
     * distinguish these two cases.
     *
     * @see #put(Object, Object)
     */
    public V get(Object key) {
        if (key == null)
            return getForNullKey();
        int hash = hash(key.hashCode());
        return get(key, hash);
    }

    V get(Object key, int hash) {
        for (long e = getFromTable(indexFor(hash, tableLength));
             e > 0l;
             e = getNext(e)) {
            Object k;
            if (getHash(e) == hash && ((k = getKey(e)) == key || key.equals(k))) {
                return getValue(e);
            }
        }
        return null;
    }

    private long getFromTable(int num) {
        return getFromTable(num, table, tableLength);
    }

    private long getFromTable(int num, long table, int tableLength) {
        if (num >= 0) {
            if (num < tableLength) {
                return unsafe.getLong(table + (num << 3));
            }
            throw new ArrayIndexOutOfBoundsException(num);
        } else {
            return 0;
        }
    }

    private void putToTable(int num, long entry) {
        unsafe.putLong(table + (num << 3), entry);
    }

    /**
     * Offloaded version of get() to look up null keys.  Null keys map
     * to index 0.  This null case is split out into separate methods
     * for the sake of performance in the two most commonly used
     * operations (get and put), but incorporated with conditionals in
     * others.
     */
    private V getForNullKey() {
        for (long e = getFromTable(0); e > 0; e = getNext(e)) {
            if (getKey(e) == null)
                return getValue(e);
        }
        return null;
    }

    /**
     * Returns <tt>true</tt> if this map contains a mapping for the
     * specified key.
     *
     * @param   key   The key whose presence in this map is to be tested
     * @return <tt>true</tt> if this map contains a mapping for the specified
     * key.
     */
    public boolean containsKey(Object key) {
        return getEntry(key) > 0;
    }

    /**
     * Returns the entry associated with the specified key in the
     * HashMap.  Returns null if the HashMap contains no mapping
     * for the key.
     */
    final long getEntry(Object key) {
        int hash = (key == null) ? 0 : hash(key.hashCode());
        return getEntry(key, hash);
    }

    long getEntry(Object key, int hash) {
        for (long e = getFromTable(indexFor(hash, tableLength));
             e > 0;
             e = getNext(e)) {
            Object k;
            if (getHash(e) == hash &&
                    ((k = getKey(e)) == key || (key != null && key.equals(k))))
                return e;
        }
        return 0;
    }


    /**
     * Associates the specified value with the specified key in this map.
     * If the map previously contained a mapping for the key, the old
     * value is replaced.
     *
     * @param key key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @return the previous value associated with <tt>key</tt>, or
     *         <tt>null</tt> if there was no mapping for <tt>key</tt>.
     *         (A <tt>null</tt> return can also indicate that the map
     *         previously associated <tt>null</tt> with <tt>key</tt>.)
     */

    public V put(K key, V value) {
        if (key == null)
            return putForNullKey(value);
        int hash = hash(key.hashCode());
        return put(key, value, hash);
    }

    V put(K key, V value, int hash) {
        int i = indexFor(hash, tableLength);
        long prev = -1;
        for (long e = getFromTable(i); e > 0; e = getNext(e)) {
            Object k;
            if (getHash(e) == hash && ((k = getKey(e)) == key || key.equals(k))) {
                long oldEntryPointer = e;
                V oldValue = getValue(e);
                e = setValue(value, e);
                if (oldEntryPointer != e) {
                     if (prev == -1) {
                         putToTable(i, e);
                     } else {
                         setNext(e, prev);
                     }
                }
                //e.recordAccess(this);
                return oldValue;
            }
            prev = e;
        }

        modCount++;
        addEntry(hash, key, value, i);
        return null;
    }

    /**
     * Offloaded version of put for null keys
     */
    private V putForNullKey(V value) {
        long e = 0;
            long prev = -1;
            for (e = getFromTable(0); e > 0; e = getNext(e)) {
                if (getKey(e) == null) {
                    long oldEntryPointer = e;
                    V oldValue = getValue(e);
                    e = setValue(value, e);
                    if (oldEntryPointer != e) {
                        if (prev == -1) {
                            putToTable(0, e);
                        } else {
                            setNext(e, prev);
                        }
                    }
                    return oldValue;
                }
                prev = e;
            }

        modCount++;
        addEntry(0, null, value, 0);
        return null;
    }

    /**
     * This method is used instead of put by constructors and
     * pseudoconstructors (clone, readObject).  It does not resize the table,
     * check for comodification, etc.  It calls createEntry rather than
     * addEntry.
     */
    private void putForCreate(K key, V value) {
        int hash = (key == null) ? 0 : hash(key.hashCode());
        int i = indexFor(hash, tableLength);

        /**
         * Look for preexisting entry for key.  This will never happen for
         * clone or deserialize.  It will only happen for construction if the
         * input Map is a sorted map whose ordering is inconsistent w/ equals.
         */
        for (long e = getFromTable(i); e > 0; e = getNext(e)) {
            Object k;
            if (getHash(e) == hash &&
                    ((k = getKey(e)) == key || (key != null && key.equals(k)))) {
                setValue(value, e);
                return;
            }
        }                                    

        createEntry(hash, key, value, i);
    }

    private void putAllForCreate(Map<? extends K, ? extends V> m) {
        for (Iterator<? extends Map.Entry<? extends K, ? extends V>> i = m.entrySet().iterator(); i.hasNext(); ) {
            Map.Entry<? extends K, ? extends V> e = i.next();
            putForCreate(e.getKey(), e.getValue());
        }
    }

    /**
     * Rehashes the contents of this map into a new array with a
     * larger capacity.  This method is called automatically when the
     * number of keys in this map reaches its threshold.
     *
     * If current capacity is MAXIMUM_CAPACITY, this method does not
     * resize the map, but sets threshold to Integer.MAX_VALUE.
     * This has the effect of preventing future calls.
     *
     * @param newCapacity the new capacity, MUST be a power of two;
     *        must be greater than current capacity unless current
     *        capacity is MAXIMUM_CAPACITY (in which case value
     *        is irrelevant).
     */
    void resize(int newCapacity) {
        int oldCapacity = tableLength;
        if (oldCapacity == MAXIMUM_CAPACITY) {
            threshold = Integer.MAX_VALUE;
            return;
        }

        long newTable = newTable(newCapacity);

        //Entry[] newTable = new Entry[newCapacity];
        transfer(oldCapacity, table, newTable, newCapacity);
        table = newTable;
        threshold = (int)(newCapacity * loadFactor);
    }

    /**
     * Transfers all entries from current table to newTable.
     */
    void transfer(int oldCapacity, long oldTable, long newTable,int newCapacity) {
        for (int j = 0; j < oldCapacity; j++) {
            long e = getFromTable(j, oldTable, oldCapacity);
            if (e > 0) {
                unsafe.putLong(oldTable + (j << 3), 0);
                do {
                    int i = indexFor(getHash(e), newCapacity);
                    long next = getNext(e);
                    setNext(unsafe.getLong(newTable + (i << 3)), e);
                    unsafe.putLong(newTable + (i << 3), e);
                    e = next;
                } while (e > 0);
            }
        }
    }

    /**
     * Copies all of the mappings from the specified map to this map.
     * These mappings will replace any mappings that this map had for
     * any of the keys currently in the specified map.
     *
     * @param m mappings to be stored in this map
     * @throws NullPointerException if the specified map is null
     */
    public void putAll(Map<? extends K, ? extends V> m) {
        int numKeysToBeAdded = m.size();
        if (numKeysToBeAdded == 0)
            return;

        /*
         * Expand the map if the map if the number of mappings to be added
         * is greater than or equal to threshold.  This is conservative; the
         * obvious condition is (m.size() + size) >= threshold, but this
         * condition could result in a map with twice the appropriate capacity,
         * if the keys to be added overlap with the keys already in this map.
         * By using the conservative calculation, we subject ourself
         * to at most one extra resize.
         */
        if (numKeysToBeAdded > threshold) {
            int targetCapacity = (int)(numKeysToBeAdded / loadFactor + 1);
            if (targetCapacity > MAXIMUM_CAPACITY)
                targetCapacity = MAXIMUM_CAPACITY;
            int newCapacity = tableLength;
            while (newCapacity < targetCapacity)
                newCapacity <<= 1;
            if (newCapacity > tableLength)
                resize(newCapacity);
        }

        for (Iterator<? extends Map.Entry<? extends K, ? extends V>> i = m.entrySet().iterator(); i.hasNext(); ) {
            Map.Entry<? extends K, ? extends V> e = i.next();
            put(e.getKey(), e.getValue());
        }
    }

    /**
     * Removes the mapping for the specified key from this map if present.
     *
     * @param  key key whose mapping is to be removed from the map
     * @return the previous value associated with <tt>key</tt>, or
     *         <tt>null</tt> if there was no mapping for <tt>key</tt>.
     *         (A <tt>null</tt> return can also indicate that the map
     *         previously associated <tt>null</tt> with <tt>key</tt>.)
     */
    public V remove(Object key) {
        long e = removeEntryForKey(key);
        return (e < 1 ? null : getValueAndFree(e));
        
    }

    V remove(Object key, int hash) {
        long e = removeEntryForKey(key, hash);
        return (e < 1 ? null : getValueAndFree(e));
       
    }

    /**
     * Removes and returns the entry associated with the specified key
     * in the HashMap.  Returns null if the HashMap contains no mapping
     * for this key.
     */
    final long removeEntryForKey(Object key) {
        int hash = (key == null) ? 0 : hash(key.hashCode());
        return removeEntryForKey(key, hash);
    }

    long removeEntryForKey(Object key, int hash) {
        int i = indexFor(hash, tableLength);
        long prev = getFromTable(i);
        long e = prev;

        while (e > 0) {
            long next = getNext(e);
            if (getHash(e) == hash &&
                    (key != null && key.equals(getKey(e)))) {
                modCount++;
                size--;
                if (prev == e) {
                    putToTable(i, next);
                } else {
                    setNext(next, prev);
                }
                // TODO recordRemoval(this);
                return e;
            }
            prev = e;
            e = next;
        }
        return e;
    }

    /**
     * Special version of remove for EntrySet.
     */
    final long removeMapping(Object o) {
        if (!(o instanceof Map.Entry))
            return 0;

        Map.Entry entry = (Map.Entry<K,V>) o;
        Object key = entry.getKey();
        int hash = (key == null) ? 0 : hash(key.hashCode());
        int i = indexFor(hash, tableLength);
        long prev = getFromTable(i);
        long e = prev;

        while (e > 0) {
            long next = getNext(e);
            if (getHash(e) == hash && equalsEntry(e, entry)) {
                modCount++;
                size--;
                if (prev == e) {
                    putToTable(i, next);
                } else {
                    setNext(next, prev);
                }
                free(e);
                return e;
            }
            prev = e;
            e = next;
        }

        return e;
    }

    /**
     * Removes all of the mappings from this map.
     * The map will be empty after this call returns.
     */
    public void clear() {
        modCount++;
        for (int i = 0; i < tableLength; i++) {
            long entry = getFromTable(i);
            if (entry > 0) {
                recursiveClear(entry);
                putToTable(i, 0);
            }
        }
        size = 0;
    }

    /**
     * Returns <tt>true</tt> if this map maps one or more keys to the
     * specified value.
     *
     * @param value value whose presence in this map is to be tested
     * @rinteturn <tt>true</tt> if this map maps one or more keys to the
     *         specified value
     */
    public boolean containsValue(Object value) {
        if (value == null)
            return containsNullValue();

        //Entry[] tab = table;
        for (int i = 0; i < tableLength ; i++)
            for (long e = getFromTable(i) ; e > 0 ; e = getNext(e))
                if (value.equals(getValue(e)))
                    return true;
        return false;
    }

    /**
     * Special-case code for containsValue with null argument
     */
    private boolean containsNullValue() {
        for (int i = 0; i < tableLength ; i++)
            for (long e = getFromTable(i) ; e > 0 ; e = getNext(e))
                if (getValue(e) == null)
                    return true;
        return false;
    }

    /**
     * Returns a shallow copy of this <tt>HashMap</tt> instance: the keys and
     * values themselves are not cloned.
     *
     * @return a shallow copy of this map
     */
    public Object clone() {
        OffHeapHashMap<K,V> result = null;
        try {
            result = (OffHeapHashMap<K,V>)super.clone();
        } catch (CloneNotSupportedException e) {
            // assert false;
        }
        result.table = newTable(tableLength);
        result.entrySet = null;
        result.modCount = 0;
        result.size = 0;
        result.init();
        result.putAllForCreate(this);

        return result;
    }
    private static final long DEF_POS = unsafe.allocateMemory(36);
    static {
        unsafe.setMemory(DEF_POS, 36, (byte)0);
    }

    private static final int KEY_START = 20;
    private static final int KEY_LENGTH = 0;
    private static final int VAL_LENGTH = 4;
    private static final int HASH = 8;
    private static final int NEXT = 12;

    public long init(int h, K k, V v, long n) {
        byte[] keyArr = toArrayK(k);
        byte[] valArr = toArrayV(v);

        int size = KEY_START + keyArr.length + valArr.length;
        long position = unsafe.allocateMemory(size);
        unsafe.setMemory(position, size, (byte) 0);
        unsafe.putInt(position + HASH, h);
        setNext(n, position);
        setKey(keyArr, position, true);
        setValueFirst(valArr, keyArr.length, position);
        return position;
    }

    protected byte[] toArrayV(V newValue) {
        return valueSerializer.serialize(newValue);
    }

    protected V fromArrayV(byte[] arr) {
        return valueSerializer.deserialize(arr);
    }

    protected byte[] toArrayK(K newKey) {
        if (newKey != null) {
            return keySerializer.serialize(newKey);
        }
        return NULL_ARR;
    }

    protected K fromArrayK(byte[] arr) {
        return keySerializer.deserialize(arr);
    }

    protected final K getKey(long position) {
        if (position < 1) {
            return null;
        }
        int length = unsafe.getInt(position + KEY_LENGTH);
        if (length < 1) {
            return null;
        }
        long keyAddr = position + KEY_START;
        byte[] key = new byte[length];
        unsafe.copyMemory(null, keyAddr, key, BYTE_ARRAY_OFFSET, length);
        return fromArrayK(key);

    }

    protected final V getValue(long position) {
        if (position < 1) {
            return null;
        }
        int length = unsafe.getInt(position + VAL_LENGTH);
        if (length < 1) {
            return null;
        }
        long valAddr = position + KEY_START + unsafe.getInt(position + KEY_LENGTH);
        return getValue0(valAddr, length);
    }

    protected V getValue0(long valAddr, int length) {
        byte[] val = new byte[length];
        unsafe.copyMemory(null, valAddr, val, BYTE_ARRAY_OFFSET, length);
        return fromArrayV(val);
    }

    protected int getHash(long position) {
        return unsafe.getInt(position + HASH);
    }

    protected long getNext(long position) {
        if (position > 0) {
            return unsafe.getLong(position + NEXT);
        }
        return 0;
    }

    protected final long setValue(V newValue, long position) {
        int keyLength = unsafe.getInt(position + KEY_LENGTH);
        long valAddr = position + KEY_START + keyLength;
        int valLength = unsafe.getInt(position + VAL_LENGTH);
        byte[] arr = toArrayV(newValue);
        int newValLength = arr.length;
        unsafe.putInt(position + VAL_LENGTH, newValLength);
        if (newValLength != valLength) {
            position = unsafe.reallocateMemory(position, KEY_START + keyLength + newValLength);
            valAddr = position + KEY_START + keyLength;
        }
        unsafe.copyMemory(arr, BYTE_ARRAY_OFFSET, null, valAddr, newValLength);
        return position;
    }

    protected final void setValueFirst(byte[] newVal, int keyLength, long position) {
        if (newVal != NULL_ARR) {
            long valAddr = position + KEY_START + keyLength;
            int length = newVal.length;
            unsafe.putInt(position + VAL_LENGTH, length);
            unsafe.copyMemory(newVal, BYTE_ARRAY_OFFSET, null, valAddr, length);
        }
    }

    protected void setKey(byte[] newKey, long position, boolean first) {
        if (newKey != NULL_ARR) {
            long keyAddr = position + KEY_START;
            int length = newKey.length;
            unsafe.putInt(position + KEY_LENGTH, length);
            unsafe.copyMemory(newKey, BYTE_ARRAY_OFFSET, null, keyAddr, length);
        }
    }

    V getValueAndFree(long position) {
        if (position == 0) {
            return null;
        }
        V ret = getValue(position);
        free(position);
        return ret;
    }

    public void free(long position) {
        if(position > 0) {
            unsafe.freeMemory(position);
        }
    }

    private void setNext(long value, long position) {
        unsafe.putLong(position + NEXT, value);
    }

    /*public void setNext(Entry<N, P> kvEntry) {
        long next = 0l;
        if (kvEntry != null && kvEntry.getPosition() > 0) {
            next = kvEntry.getPosition();
        }
        setNext(next);
    }*/

    public void recursiveClear(long position) {
        long next = getNext(position);
        if (next > 0) {
            recursiveClear(next);
        }
        free(position);
    }


    public final boolean equalsEntry(long entry, Object o) {
        if (!(o instanceof Map.Entry))
            return false;
        Map.Entry e = (Map.Entry)o;
        Object k1 = getKey(entry);
        Object k2 = e.getKey();
        if (k1 == k2 || (k1 != null && k1.equals(k2))) {
            Object v1 = getValue(entry);
            Object v2 = e.getValue();
            if (v1 == v2 || (v1 != null && v1.equals(v2)))
                return true;
        }
        return false;
    }
    
    class Entry implements Map.Entry<K,V> {
        private long position ;

        public Entry(long position) {
            this.position = position;
        }

        void setPosition(long position) {
            this.position = position;
        }

        long getPosition() {
            return position;
        }

        @Override
        public K getKey() {
            return OffHeapHashMap.this.getKey(position);
        }

        @Override
        public V getValue() {
            return OffHeapHashMap.this.getValue(position);
        }

        @Override
        public V setValue(V value) {
            V ret = getValue();
            long newPointer = OffHeapHashMap.this.setValue(value, position);
            if (newPointer != position) {
                OffHeapHashMap.this.updateEntry(position, newPointer);
            }
            return ret;
        }

        public final boolean equals(Object o) {
            return OffHeapHashMap.this.equalsEntry(position, o);
        }

        public final int hashCode() {
            K key = getKey();
            V value = getValue();
            return (key ==null   ? 0 : key.hashCode()) ^
                    (value ==null ? 0 : value.hashCode());
        }

        public final String toString() {
            return String.valueOf(getKey()) + "=" + String.valueOf(getValue());
            //return String.valueOf(position); FOR DEBUG
        }

        /**
         * This method is invoked whenever the value in an entry is
         * overwritten by an invocation of put(k,v) for a key k that's already
         * in the HashMap.
         */
        void recordAccess(OffHeapHashMap<K,V> m) {
        }

        /**
         * This method is invoked whenever the entry is
         * removed from the table.
         */
    }

    int getTableLength() {
        return tableLength;
    }

    Entry getEntryAtPosition(int tabPosition, int num) {
        if (tabPosition >= 0 && tabPosition < tableLength) {
            long ret = getFromTable(tabPosition);
            while (ret > 0 && num-- > 0) {
                ret = getNext(ret);
            }
            if (ret > 0) {
                return new Entry(ret);
            } else {
                return null;
            }
        } else {
            throw new IllegalStateException();
        }
    }

    private void updateEntry(long oldPos, long newPos) {
        long prev = -1;
        int hash = getHash(newPos);
        int num = indexFor(hash, tableLength);
        for (long e = getFromTable(num); e > 0; e = getNext(e)) {
            if (e == oldPos) {
                if (prev > -1) {
                    setNext(newPos, e);
                    break;
                } else {
                    putToTable(num, newPos);
                }
            }
            prev = e;
        }
    }

    /**
     * Adds a new entry with the specified key, value and hash code to
     * the specified bucket.  It is the responsibility of this
     * method to resize the table if appropriate.
     *
     * Subclass overrides this to alter the behavior of put method.
     */
    void addEntry(int hash, K key, V value, int bucketIndex) {
        long e = getFromTable(bucketIndex);
        long entry = init(hash, key, value, e);
        putToTable(bucketIndex, entry);
        if (size++ >= threshold)
            resize(2 * tableLength);
    }

    /**
     * Like addEntry except that this version is used when creating entries
     * as part of Map construction or "pseudo-construction" (cloning,
     * deserialization).  This version needn't worry about resizing the table.
     *
     * Subclass overrides this to alter the behavior of HashMap(Map),
     * clone, and readObject.
     */
    void createEntry(int hash, K key, V value, int bucketIndex) {
        long e = getFromTable(bucketIndex);
        long entry = init(hash, key, value, e);
        putToTable(bucketIndex, entry);
        size++;
    }

    private abstract class HashIterator<E> implements Iterator<E> {
        long next;	// next entry to return
        int expectedModCount;	// For fast-fail
        int index;		// current slot
        long current;	// current entry

        HashIterator() {
            expectedModCount = modCount;
            if (size > 0) { // advance to first entry
                //Entry[] t = table;
                while (index < tableLength && (next = getFromTable(index++)) == 0)
                    ;
            }
        }

        public final boolean hasNext() {
            return next > 0;
        }

        final long nextEntry() {
            if (modCount != expectedModCount)
                throw new ConcurrentModificationException();
            long e = next;
            if (e < 1)
                throw new NoSuchElementException();

            if ((next = getNext(e)) == 0) {
                while (index < tableLength && (next = getFromTable(index++)) == 0)
                    ;
            }
            current = e;
            return e;
        }

        public void remove() {
            if (current == 0)
                throw new IllegalStateException();
            if (modCount != expectedModCount)
                throw new ConcurrentModificationException();
            Object k = getKey(current);
            free(OffHeapHashMap.this.removeEntryForKey(k));
            expectedModCount = modCount;
        }

    }

    private final class ValueIterator extends HashIterator<V> {
        public V next() {
            return getValue(nextEntry());
        }
    }

    private final class KeyIterator extends HashIterator<K> {
        public K next() {
            return getKey(nextEntry());
        }
    }

    private final class EntryIterator extends HashIterator<Map.Entry<K,V>> {
        public Map.Entry<K,V> next() {
            return new Entry(nextEntry());
        }
    }

    // Subclass overrides these to alter behavior of views' iterator() method
    Iterator<K> newKeyIterator()   {
        return new KeyIterator();
    }
    Iterator<V> newValueIterator()   {
        return new ValueIterator();
    }
    Iterator<Map.Entry<K,V>> newEntryIterator()   {
        return new EntryIterator();
    }


    // Views

    private transient Set<Map.Entry<K,V>> entrySet = null;

    /**
     * Returns a {@link java.util.Set} view of the keys contained in this map.
     * The set is backed by the map, so changes to the map are
     * reflected in the set, and vice-versa.  If the map is modified
     * while an iteration over the set is in progress (except through
     * the iterator's own <tt>remove</tt> operation), the results of
     * the iteration are undefined.  The set supports element removal,
     * which removes the corresponding mapping from the map, via the
     * <tt>Iterator.remove</tt>, <tt>Set.remove</tt>,
     * <tt>removeAll</tt>, <tt>retainAll</tt>, and <tt>clear</tt>
     * operations.  It does not support the <tt>add</tt> or <tt>addAll</tt>
     * operations.
     */
    public Set<K> keySet() {
        return new KeySet();
    }

    private final class KeySet extends AbstractSet<K> {
        public Iterator<K> iterator() {
            return newKeyIterator();
        }
        public int size() {
            return size;
        }
        public boolean contains(Object o) {
            return containsKey(o);
        }
        public boolean remove(Object o) {
            long kvEntry = OffHeapHashMap.this.removeEntryForKey(o);
            free(kvEntry);
            return kvEntry > 0;
        }
        public void clear() {
            OffHeapHashMap.this.clear();
        }
    }

    /**
     * Returns a {@link java.util.Collection} view of the values contained in this map.
     * The collection is backed by the map, so changes to the map are
     * reflected in the collection, and vice-versa.  If the map is
     * modified while an iteration over the collection is in progress
     * (except through the iterator's own <tt>remove</tt> operation),
     * the results of the iteration are undefined.  The collection
     * supports element removal, which removes the corresponding
     * mapping from the map, via the <tt>Iterator.remove</tt>,
     * <tt>Collection.remove</tt>, <tt>removeAll</tt>,
     * <tt>retainAll</tt> and <tt>clear</tt> operations.  It does not
     * support the <tt>add</tt> or <tt>addAll</tt> operations.
     */
    public Collection<V> values() {
        //Collection<V> vs = values;
        //return (vs != null ? vs : (values = new Values()));
        return new Values();
    }

    private final class Values extends AbstractCollection<V> {
        public Iterator<V> iterator() {
            return newValueIterator();
        }
        public int size() {
            return size;
        }
        public boolean contains(Object o) {
            return containsValue(o);
        }
        public void clear() {
            OffHeapHashMap.this.clear();
        }
    }

    /**
     * Returns a {@link java.util.Set} view of the mappings contained in this map.
     * The set is backed by the map, so changes to the map are
     * reflected in the set, and vice-versa.  If the map is modified
     * while an iteration over the set is in progress (except through
     * the iterator's own <tt>remove</tt> operation, or through the
     * <tt>setValueFirst</tt> operation on a map entry returned by the
     * iterator) the results of the iteration are undefined.  The set
     * supports element removal, which removes the corresponding
     * mapping from the map, via the <tt>Iterator.remove</tt>,
     * <tt>Set.remove</tt>, <tt>removeAll</tt>, <tt>retainAll</tt> and
     * <tt>clear</tt> operations.  It does not support the
     * <tt>add</tt> or <tt>addAll</tt> operations.
     *
     * @return a set view of the mappings contained in this map
     */
    public Set<Map.Entry<K,V>> entrySet() {
        return entrySet0();
    }

    private Set<Map.Entry<K,V>> entrySet0() {
        Set<Map.Entry<K,V>> es = entrySet;
        return es != null ? es : (entrySet = new EntrySet());
    }

    private final class EntrySet extends AbstractSet<Map.Entry<K,V>> {
        public Iterator<Map.Entry<K,V>> iterator() {
            return newEntryIterator();
        }
        public boolean contains(Object o) {
            if (!(o instanceof Map.Entry))
                return false;
            Map.Entry e = (Map.Entry<K,V>) o;
            long candidate = getEntry(e.getKey());
            return candidate > 0 && OffHeapHashMap.this.equalsEntry(candidate ,e);
        }
        public boolean remove(Object o) {
            return removeMapping(o) > 0;
        }
        public int size() {
            return size;
        }
        public void clear() {
            OffHeapHashMap.this.clear();
        }
    }

    /**
     * Save the state of the <tt>HashMap</tt> instance to a stream (i.e.,
     * serialize it).
     *
     * @serialData The <i>capacity</i> of the HashMap (the length of the
     *		   bucket array) is emitted (int), followed by the
     *		   <i>size</i> (an int, the number of key-value
     *		   mappings), followed by the key (Object) and value (Object)
     *		   for each key-value mapping.  The key-value mappings are
     *		   emitted in no particular order.
     */
    private void writeObject(java.io.ObjectOutputStream s)
            throws IOException
    {
        Iterator<Map.Entry<K,V>> i =
                (size > 0) ? entrySet0().iterator() : null;

        // Write out the threshold, loadfactor, and any hidden stuff
        s.defaultWriteObject();

        // Write out number of buckets
        s.writeInt(tableLength);

        // Write out size (number of Mappings)
        s.writeInt(size);

        // Write out keys and values (alternating)
        if (i != null) {
            while (i.hasNext()) {
                Map.Entry e = i.next();
                s.writeObject(e.getKey());
                s.writeObject(e.getValue());
            }
        }
    }

    /**
     * Reconstitute the <tt>HashMap</tt> instance from a stream (i.e.,
     * deserialize it).
     */
    private void readObject(java.io.ObjectInputStream s)
            throws IOException, ClassNotFoundException
    {
        // Read in the threshold, loadfactor, and any hidden stuff
        s.defaultReadObject();

        // Read in number of buckets and allocate the bucket array;
        int numBuckets = s.readInt();
        table = newTable(numBuckets);

        init();  // Give subclass a chance to do its thing.

        // Read in size (number of Mappings)
        int size = s.readInt();

        // Read the keys and values, and put the mappings in the HashMap
        for (int i=0; i<size; i++) {
            K key = (K) s.readObject();
            V value = (V) s.readObject();
            putForCreate(key, value);
        }
    }

    // These methods are used when serializing HashSets
    int   capacity()     { return tableLength; }
    float loadFactor()   { return loadFactor;   }



}
