package com.jad.offheap.collections.impl;


import com.jad.offheap.UnsafeWrapper;
import com.jad.offheap.collections.serializer.Serializer;

import java.io.IOException;
import java.util.*;


/**
 * @author: Ilya Krokhmalyov jad7kii@gmail.com
 * @since: 12/15/13
 */

public class OffHeapHashSet<K> extends AbstractSet<K>
        implements Set<K>, Cloneable {
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

    private boolean containNull = false; //TODO null not support on iterator

    /**
     * Constructs an empty <tt>HashMap</tt> with the specified initial
     * capacity and load factor.
     *
     * @param  initialCapacity the initial capacity
     * @param  loadFactor      the load factor
     * @throws IllegalArgumentException if the initial capacity is negative
     *         or the load factor is nonpositive
     */
    public OffHeapHashSet(int initialCapacity, float loadFactor, Serializer<K> keySerializer) {
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
    public OffHeapHashSet(int initialCapacity, Serializer<K> keySerializer) {
        this(initialCapacity, DEFAULT_LOAD_FACTOR, keySerializer);
    }

    /**
     * Constructs an empty <tt>HashMap</tt> with the default initial capacity
     * (16) and the default load factor (0.75).
     */
    public OffHeapHashSet(Serializer<K> keySerializer) {
        this.loadFactor = DEFAULT_LOAD_FACTOR;
        threshold = (int)(DEFAULT_INITIAL_CAPACITY * DEFAULT_LOAD_FACTOR);
        table = newTable(DEFAULT_INITIAL_CAPACITY);
        this.keySerializer = keySerializer;
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
    public OffHeapHashSet(Collection<? extends K> m,Serializer<K> keySerializer) {
        this(Math.max((int) (m.size() / DEFAULT_LOAD_FACTOR) + 1,
                DEFAULT_INITIAL_CAPACITY), DEFAULT_LOAD_FACTOR, keySerializer);
        putAllForCreate(m);
    }

    public Serializer<K> getKeySerializer() {
        return keySerializer;
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
     * Returns <tt>true</tt> if this map contains a mapping for the
     * specified key.
     *
     * @param   key   The key whose presence in this map is to be tested
     * @return <tt>true</tt> if this map contains a mapping for the specified
     * key.
     */
    public boolean containsKey(Object key) {
        if (key == null) return containNull;
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


    public boolean add(K key) {
        if (key == null) {
            if (containNull) {
                return false;
            } else {
                return containNull = true;
            }
        }
        int hash = hash(key.hashCode());
        return add(key, hash);
    }

    boolean add(K key, int hash) {
        int i = indexFor(hash, tableLength);
        for (long e = getFromTable(i); e > 0; e = getNext(e)) {
            if (getHash(e) == hash && key.equals(getKey(e))) {
                return false;
            }
        }

        modCount++;
        addEntry(hash, key, i);
        return true;
    }

    /**
     * This method is used instead of put by constructors and
     * pseudoconstructors (clone, readObject).  It does not resize the table,
     * check for comodification, etc.  It calls createEntry rather than
     * addEntry.
     */
    private void putForCreate(K key) {
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
                return;
            }
        }

        createEntry(hash, key, i);
    }

    private void putAllForCreate(Collection<? extends K> m) {
        for (Iterator<? extends K> i = m.iterator(); i.hasNext(); ) {
            K e = i.next();
            putForCreate(e);
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
    public boolean addAll(Collection<? extends K> m) {
        int numKeysToBeAdded = m.size();
        if (numKeysToBeAdded == 0)
            return false;

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

        boolean mod = false;
        for (Iterator<? extends K> i = m.iterator(); i.hasNext(); ) {
            K e = i.next();
            if (add(e)) {
                mod = true;
            }
        }
        return mod;
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
    public boolean remove(Object key) {
        long e = removeEntryForKey(key);
        return (e > 0);

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
        if (key == null) {
            containNull = false;
            return 1;
        }
        int i = indexFor(hash, tableLength);
        long prev = getFromTable(i);
        long e = prev;

        while (e > 0) {
            long next = getNext(e);
            Object k;
            if (getHash(e) == hash && key.equals(getKey(e))) {
                modCount++;
                size--;
                if (prev == e) {
                    putToTable(i, next);
                } else {
                    setNext(next, prev);
                }
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
     * Returns a shallow copy of this <tt>HashMap</tt> instance: the keys and
     * values themselves are not cloned.
     *
     * @return a shallow copy of this map
     */
    public Object clone() {
        OffHeapHashSet<K> result = null;
        try {
            result = (OffHeapHashSet<K>)super.clone();
        } catch (CloneNotSupportedException e) {
            // assert false;
        }
        result.table = newTable(tableLength);
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

    private static final int KEY_START = 16;
    private static final int KEY_LENGTH = 0;
    private static final int HASH = 4;
    private static final int NEXT = 8;

    public long init(int h, K k, long n) {
        byte[] keyArr = toArrayK(k);

        int size = KEY_START + keyArr.length;
        long position = unsafe.allocateMemory(size);
        unsafe.setMemory(position, size, (byte) 0);
        unsafe.putInt(position + HASH, h);
        setNext(n, position);
        setKey(keyArr, position, true);
        return position;
    }

    protected byte[] toArrayK(K newKey) {
        return keySerializer.serialize(newKey);
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

    protected int getHash(long position) {
        return unsafe.getInt(position + HASH);
    }

    protected long getNext(long position) {
        if (position > 0) {
            return unsafe.getLong(position + NEXT);
        }
        return 0;
    }

    protected void setKey(byte[] newKey, long position, boolean first) {
        if (newKey != NULL_ARR) {
            long keyAddr = position + KEY_START;
            int length = newKey.length;
            unsafe.putInt(position + KEY_LENGTH, length);
            unsafe.copyMemory(newKey, BYTE_ARRAY_OFFSET, null, keyAddr, length);
        }
    }

    public void free(long position) {
        if(position > 0) {
            unsafe.freeMemory(position);
        }
    }

    private void setNext(long value, long position) {
        unsafe.putLong(position + NEXT, value);
    }

    public void recursiveClear(long position) {
        long next = getNext(position);
        if (next > 0) {
            recursiveClear(next);
        }
        free(position);
    }


    /**
     * Adds a new entry with the specified key, value and hash code to
     * the specified bucket.  It is the responsibility of this
     * method to resize the table if appropriate.
     *
     * Subclass overrides this to alter the behavior of put method.
     */
    void addEntry(int hash, K key, int bucketIndex) {
        long e = getFromTable(bucketIndex);
        long entry = init(hash, key, e);
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
    void createEntry(int hash, K key, int bucketIndex) {
        long e = getFromTable(bucketIndex);
        long entry = init(hash, key, e);
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
            free(OffHeapHashSet.this.removeEntryForKey(k));
            expectedModCount = modCount;
        }

    }

    private final class KeyIterator extends HashIterator<K> {
        public K next() {
            return getKey(nextEntry());
        }
    }

    public Iterator<K> iterator()   {
        return new KeyIterator();
    }

    // Views

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
        Iterator<K> i =
                (size > 0) ? iterator() : null;

        // Write out the threshold, loadfactor, and any hidden stuff
        s.defaultWriteObject();

        // Write out number of buckets
        s.writeInt(tableLength);

        // Write out size (number of Mappings)
        s.writeInt(size);

        // Write out keys and values (alternating)
        if (i != null) {
            while (i.hasNext()) {
                s.writeObject(i.next());
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
            putForCreate(key);
        }
    }

    // These methods are used when serializing HashSets
    int   capacity()     { return tableLength; }
    float loadFactor()   { return loadFactor;   }



}
