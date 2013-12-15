package com.jad.offheap.collections.impl;

import com.jad.offheap.collections.serializer.Serializers;
import org.apache.commons.lang.RandomStringUtils;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

/**
 * @author: Ilya Krokhmalyov YC14IK1
 * @since: 12/15/13
 */

public class SimpleUsageSet {

    @Test
    public void simpleTest() {
        Set<Integer> map = new OffHeapHashSet<Integer>(1000, 0.75f
                , Serializers.INT_SERIALIZER);

        long start = System.currentTimeMillis();
        for (int index = 0; index < 1000; index++) {
            map.add(index);
        }
        System.out.println("Time:" + (System.currentTimeMillis() - start));
        System.out.println(map);
        map.clear();
    }

}
