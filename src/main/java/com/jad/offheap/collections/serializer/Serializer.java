package com.jad.offheap.collections.serializer;

/**
 * @author: Ilya Krokhmalyov jad7kii@gmail.com
 * @since: 10/5/13
 */

public interface Serializer<K> {

    byte[] serialize(K key);

    K deserialize(byte[] bytes);

}
