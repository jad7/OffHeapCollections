package com.jad.offheap.collections.utils;

import static java.lang.String.format;

/**
 * @author: Ilya Krokhmalyov jad7kii@gmail.com
 * @since: 10/6/13
 */

public class Utils {

    public static long byteArrToLong(byte[] buf) {
        return  ((buf[0] & 0xFFL) << 56) |
                ((buf[1] & 0xFFL) << 48) |
                ((buf[2] & 0xFFL) << 40) |
                ((buf[3] & 0xFFL) << 32) |
                ((buf[4] & 0xFFL) << 24) |
                ((buf[5] & 0xFFL) << 16) |
                ((buf[6] & 0xFFL) <<  8) |
                ((buf[7] & 0xFFL));
    }

    public static int byteArrToInt(byte[] buf) {
        return  ((buf[0] & 0xFF) << 24) |
                ((buf[1] & 0xFF) << 16) |
                ((buf[2] & 0xFF) <<  8) |
                ((buf[3] & 0xFF));
    }

    public static byte[] longToByteArr(long v) {
        return new byte[] {
                (byte)(v >>> 56),
                (byte)(v >>> 48),
                (byte)(v >>> 40),
                (byte)(v >>> 32),
                (byte)(v >>> 24),
                (byte)(v >>> 16),
                (byte)(v >>>  8),
                (byte)(v)
        };
    }

    public static byte[] intToByteArr(int v) {
        return new byte[] {
                (byte)(v >>> 24),
                (byte)(v >>> 16),
                (byte)(v >>>  8),
                (byte)(v)
        };
    }

    public static long byteArrToLong(byte[] buf, int position) {
        return  ((buf[position++] & 0xFFL) << 56) |
                ((buf[position++] & 0xFFL) << 48) |
                ((buf[position++] & 0xFFL) << 40) |
                ((buf[position++] & 0xFFL) << 32) |
                ((buf[position++] & 0xFFL) << 24) |
                ((buf[position++] & 0xFFL) << 16) |
                ((buf[position++] & 0xFFL) <<  8) |
                ((buf[position] & 0xFFL));
    }

    public static int byteArrToInt(byte[] buf, int position) {
        return (int) (((buf[position++] & 0xFFL) << 24) |
                        ((buf[position++] & 0xFFL) << 16) |
                        ((buf[position++] & 0xFFL) <<  8) |
                        ((buf[position] & 0xFFL)));
    }

    public static byte[] longToByteArr(long v, byte[] arr, int position) {
        for (int i = 56; i >= 0; i-=8) {
            arr[position--] = (byte)(v >>> i);
        }
        return arr;
    }

    public static byte[] intToByteArr(int v, byte[] arr, int position) {
        for (int i = 24; i >= 0; i-=8) {
            arr[position--] = (byte)(v >>> i);
        }
        return arr;
    }

    public static void main(String[] args) {
        System.out.println(byteArrToLong(longToByteArr(-1l)));
    }

    public static void checkNotNegative(long i, String param) {
        if (i < 0) {
            throw new IllegalArgumentException(format("Parameter %s should be not negative", param));
        }
    }

    public static void checkPositive(long i, String param) {
        if (i < 1) {
            throw new IllegalArgumentException(format("Parameter %s should be positive", param));
        }
    }

    public static void checkNotNull(Object o, String param) {
        if (o == null) {
            throw new IllegalArgumentException(format("Parameter %s should be not null", param));
        }
    }


}
