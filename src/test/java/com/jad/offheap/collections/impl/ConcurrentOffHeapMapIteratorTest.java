package com.jad.offheap.collections.impl;

import com.jad.offheap.collections.serializer.Serializers;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * @author: Ilya Krokhmalyov YC14IK1
 * @since: 12/22/13
 */

@Test
public class ConcurrentOffHeapMapIteratorTest {

    @Test
    public void testMainCase() {
        int initialCapacity = 1000;
        ConcurrentOffHeapMap<Integer, Integer> map = new ConcurrentOffHeapMap<Integer, Integer>(initialCapacity, 0.75f, 16, Serializers.INT_SERIALIZER, Serializers.INT_SERIALIZER);
        Set<Integer> primitiveSet = new HashSet<Integer>(initialCapacity);
        for (int index = 0; index < initialCapacity; index++) {
            map.put(index, index);
            primitiveSet.add(index);
        }

        Iterator<Map.Entry<Integer,Integer>> iterator = map.entrySet().iterator();
        int index = 0;
        int removeFactor = 40;
        while (iterator.hasNext()) {
            Map.Entry<Integer, Integer> next = iterator.next();
            Integer key = next.getKey();
            Assert.assertTrue(primitiveSet.contains(key));
            primitiveSet.remove(key);
            if (++index % removeFactor == 0) {
                iterator.remove();
            }
        }
        Assert.assertEquals(primitiveSet.size(), 0);
        Assert.assertEquals(map.size(), initialCapacity - initialCapacity / removeFactor);

    }
}
