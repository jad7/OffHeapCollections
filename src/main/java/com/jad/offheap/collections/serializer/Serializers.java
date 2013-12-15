package com.jad.offheap.collections.serializer;

import com.jad.offheap.collections.utils.Utils;

import java.io.*;

/**
 * @author: Ilya Krokhmalyov
 * @email jad7kii@gmail.com
 * @since: 12/15/13
 */

public class Serializers {

    public static Serializer<Long> LONG_SERIALIZER = new Serializer<Long>() {
        @Override
        public byte[] serialize(Long key) {
            return Utils.longToByteArr(key);
        }

        @Override
        public Long deserialize(byte[] bytes) {
            return Utils.byteArrToLong(bytes);
        }
    };

    public static Serializer<Integer> INT_SERIALIZER = new Serializer<Integer>() {
        @Override
        public byte[] serialize(Integer key) {
            return Utils.intToByteArr(key);
        }

        @Override
        public Integer deserialize(byte[] bytes) {
            return Utils.byteArrToInt(bytes);
        }
    };

    public static Serializer<String> STRING_SERIALIZER = new Serializer<String>() {
        @Override
        public byte[] serialize(String key) {
            try {
                return key.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public String deserialize(byte[] bytes) {
            try {
                return new String(bytes, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        }
    };

    public static Serializer<Serializable> SERIALIZABLE_SERIALIZER = new SerializableSerializer();

    public static class SerializableSerializer<T extends Serializable> implements Serializer<T> {
        @Override
        public byte[] serialize(T key) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            try {
                ObjectOutputStream oos = new ObjectOutputStream(bos);
                oos.writeObject(key);
                oos.flush();
            } catch (Exception e) {
                throw new SerializationException(e);
            }
            return bos.toByteArray();
        }

        @Override
        public T deserialize(byte[] bytes) {
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            try {
                return (T) new ObjectInputStream(bis).readObject();
            } catch (Exception e) {
                throw new SerializationException(e);
            }
        }
    }
}
