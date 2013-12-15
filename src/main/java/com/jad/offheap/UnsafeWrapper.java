package com.jad.offheap;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.security.ProtectionDomain;

/**
 * @author: Ilya Krokhmalyov jad7kii@gmail.com
 * @since: 12/15/13
 */

public class UnsafeWrapper {
    private Unsafe unsafe = UnsafeProvider.getUnsafe();

    public static final UnsafeWrapper unsafeWrapper = new UnsafeWrapper();

    private UnsafeWrapper() {
    }

    public int addressSize() {
        return unsafe.addressSize();
    }

    public float getFloat(long position) {
        return unsafe.getFloat(position);
    }

    @Deprecated
    public void putDouble(Object o, int i, double v) {
        unsafe.putDouble(o, i, v);
    }

    public void ensureClassInitialized(Class aClass) {
        unsafe.ensureClassInitialized(aClass);
    }

    public short getShort(long position) {
        return unsafe.getShort(position);
    }

    public static Unsafe getUnsafe() {
        return Unsafe.getUnsafe();
    }

    public boolean getBooleanVolatile(Object o, long position) {
        return unsafe.getBooleanVolatile(o, position);
    }

    public short getShort(Object o, long position) {
        return unsafe.getShort(o, position);
    }

    public double getDouble(Object o, long position) {
        return unsafe.getDouble(o, position);
    }

    public void putBoolean(Object o, long position, boolean b) {
        unsafe.putBoolean(o, position, b);
    }

    public void putLongVolatile(Object o, long position, long position2) {
        unsafe.putLongVolatile(o, position, position2);
    }

    public void freeMemory(long position) {
        unsafe.freeMemory(position);
    }

    @Deprecated
    public void putLong(Object o, int i, long position) {
        unsafe.putLong(o, i, position);
    }

    @Deprecated
    public void putBoolean(Object o, int i, boolean b) {
        unsafe.putBoolean(o, i, b);
    }

    public long objectFieldOffset(Field field) {
        return unsafe.objectFieldOffset(field);
    }

    public void putFloat(Object o, long position, float v) {
        unsafe.putFloat(o, position, v);
    }

    @Deprecated
    public double getDouble(Object o, int i) {
        return unsafe.getDouble(o, i);
    }

    @Deprecated
    public char getChar(Object o, int i) {
        return unsafe.getChar(o, i);
    }

    @Deprecated
    public void putInt(Object o, int i, int i2) {
        unsafe.putInt(o, i, i2);
    }

    public void putCharVolatile(Object o, long position, char c) {
        unsafe.putCharVolatile(o, position, c);
    }

    public void copyMemory(long position, long position2, long position3) {
        unsafe.copyMemory(position, position2, position3);
    }

    public void putLong(long position, long position2) {
        unsafe.putLong(position, position2);
    }

    public boolean compareAndSwapLong(Object o, long position, long position2, long position3) {
        return unsafe.compareAndSwapLong(o, position, position2, position3);
    }

    public void putInt(long position, int i) {
        unsafe.putInt(position, i);
    }

    public short getShortVolatile(Object o, long position) {
        return unsafe.getShortVolatile(o, position);
    }

    @Deprecated
    public byte getByte(Object o, int i) {
        return unsafe.getByte(o, i);
    }

    public void putShort(Object o, long position, short i) {
        unsafe.putShort(o, position, i);
    }

    @Deprecated
    public int fieldOffset(Field field) {
        return unsafe.fieldOffset(field);
    }

    public boolean compareAndSwapInt(Object o, long position, int i, int i2) {
        return unsafe.compareAndSwapInt(o, position, i, i2);
    }

    public void putFloatVolatile(Object o, long position, float v) {
        unsafe.putFloatVolatile(o, position, v);
    }

    @Deprecated
    public boolean getBoolean(Object o, int i) {
        return unsafe.getBoolean(o, i);
    }

    public int getInt(long position) {
        return unsafe.getInt(position);
    }

    public char getCharVolatile(Object o, long position) {
        return unsafe.getCharVolatile(o, position);
    }

    public void putByteVolatile(Object o, long position, byte b) {
        unsafe.putByteVolatile(o, position, b);
    }

    public Class defineClass(String s, byte[] bytes, int i, int i2) {
        return unsafe.defineClass(s, bytes, i, i2);
    }

    public float getFloatVolatile(Object o, long position) {
        return unsafe.getFloatVolatile(o, position);
    }

    public float getFloat(Object o, long position) {
        return unsafe.getFloat(o, position);
    }

    public Object allocateInstance(Class aClass) throws InstantiationException {
        return unsafe.allocateInstance(aClass);
    }

    public void putDouble(long position, double v) {
        unsafe.putDouble(position, v);
    }

    public int getInt(Object o, long position) {
        return unsafe.getInt(o, position);
    }

    public long reallocateMemory(long position, long position2) {
        return unsafe.reallocateMemory(position, position2);
    }

    public void putChar(long position, char c) {
        unsafe.putChar(position, c);
    }

    public int arrayBaseOffset(Class aClass) {
        return unsafe.arrayBaseOffset(aClass);
    }

    public long getLong(Object o, long position) {
        return unsafe.getLong(o, position);
    }

    public char getChar(long position) {
        return unsafe.getChar(position);
    }

    @Deprecated
    public float getFloat(Object o, int i) {
        return unsafe.getFloat(o, i);
    }

    public void putOrderedLong(Object o, long position, long position2) {
        unsafe.putOrderedLong(o, position, position2);
    }

    public void copyMemory(Object o, long position, Object o2, long position2, long position3) {
        unsafe.copyMemory(o, position, o2, position2, position3);
    }

    public void monitorEnter(Object o) {
        unsafe.monitorEnter(o);
    }

    @Deprecated
    public void putObject(Object o, int i, Object o2) {
        unsafe.putObject(o, i, o2);
    }

    public long getLongVolatile(Object o, long position) {
        return unsafe.getLongVolatile(o, position);
    }

    public void putOrderedInt(Object o, long position, int i) {
        unsafe.putOrderedInt(o, position, i);
    }

    @Deprecated
    public Object staticFieldBase(Class aClass) {
        return unsafe.staticFieldBase(aClass);
    }

    public void putIntVolatile(Object o, long position, int i) {
        unsafe.putIntVolatile(o, position, i);
    }

    public long getLong(long position) {
        return unsafe.getLong(position);
    }

    public void putInt(Object o, long position, int i) {
        unsafe.putInt(o, position, i);
    }

    public void putOrderedObject(Object o, long position, Object o2) {
        unsafe.putOrderedObject(o, position, o2);
    }

    public void monitorExit(Object o) {
        unsafe.monitorExit(o);
    }

    public double getDouble(long position) {
        return unsafe.getDouble(position);
    }

    public void putByte(Object o, long position, byte b) {
        unsafe.putByte(o, position, b);
    }

    public boolean getBoolean(Object o, long position) {
        return unsafe.getBoolean(o, position);
    }

    public void putShort(long position, short i) {
        unsafe.putShort(position, i);
    }

    public byte getByte(long position) {
        return unsafe.getByte(position);
    }

    public void unpark(Object o) {
        unsafe.unpark(o);
    }

    public void throwException(Throwable throwable) {
        unsafe.throwException(throwable);
    }

    @Deprecated
    public short getShort(Object o, int i) {
        return unsafe.getShort(o, i);
    }

    public Object getObject(Object o, long position) {
        return unsafe.getObject(o, position);
    }

    public char getChar(Object o, long position) {
        return unsafe.getChar(o, position);
    }

    public byte getByte(Object o, long position) {
        return unsafe.getByte(o, position);
    }

    @Deprecated
    public void putShort(Object o, int i, short i2) {
        unsafe.putShort(o, i, i2);
    }

    public void putLong(Object o, long position, long position2) {
        unsafe.putLong(o, position, position2);
    }

    @Deprecated
    public Object getObject(Object o, int i) {
        return unsafe.getObject(o, i);
    }

    public void putObject(Object o, long position, Object o2) {
        unsafe.putObject(o, position, o2);
    }

    @Deprecated
    public void putFloat(Object o, int i, float v) {
        unsafe.putFloat(o, i, v);
    }

    @Deprecated
    public void putByte(Object o, int i, byte b) {
        unsafe.putByte(o, i, b);
    }

    public void putByte(long position, byte b) {
        unsafe.putByte(position, b);
    }

    public long staticFieldOffset(Field field) {
        return unsafe.staticFieldOffset(field);
    }

    public Object getObjectVolatile(Object o, long position) {
        return unsafe.getObjectVolatile(o, position);
    }

    public byte getByteVolatile(Object o, long position) {
        return unsafe.getByteVolatile(o, position);
    }

    public void putFloat(long position, float v) {
        unsafe.putFloat(position, v);
    }

    public void setMemory(long position, long position2, byte b) {
        unsafe.setMemory(position, position2, b);
    }

    @Deprecated
    public long getLong(Object o, int i) {
        return unsafe.getLong(o, i);
    }

    public void putObjectVolatile(Object o, long position, Object o2) {
        unsafe.putObjectVolatile(o, position, o2);
    }

    @Deprecated
    public int getInt(Object o, int i) {
        return unsafe.getInt(o, i);
    }

    public boolean compareAndSwapObject(Object o, long position, Object o2, Object o3) {
        return unsafe.compareAndSwapObject(o, position, o2, o3);
    }

    @Deprecated
    public void putChar(Object o, int i, char c) {
        unsafe.putChar(o, i, c);
    }

    public int arrayIndexScale(Class aClass) {
        return unsafe.arrayIndexScale(aClass);
    }

    public void putShortVolatile(Object o, long position, short i) {
        unsafe.putShortVolatile(o, position, i);
    }

    public int getIntVolatile(Object o, long position) {
        return unsafe.getIntVolatile(o, position);
    }

    public void putDoubleVolatile(Object o, long position, double v) {
        unsafe.putDoubleVolatile(o, position, v);
    }

    public long allocateMemory(long position) {
        return unsafe.allocateMemory(position);
    }

    public double getDoubleVolatile(Object o, long position) {
        return unsafe.getDoubleVolatile(o, position);
    }

    public long getAddress(long position) {
        return unsafe.getAddress(position);
    }

    public void park(boolean b, long position) {
        unsafe.park(b, position);
    }

    public Class defineClass(String s, byte[] bytes, int i, int i2, ClassLoader classLoader, ProtectionDomain protectionDomain) {
        return unsafe.defineClass(s, bytes, i, i2, classLoader, protectionDomain);
    }

    public void putChar(Object o, long position, char c) {
        unsafe.putChar(o, position, c);
    }

    public boolean tryMonitorEnter(Object o) {
        return unsafe.tryMonitorEnter(o);
    }

    public void putBooleanVolatile(Object o, long position, boolean b) {
        unsafe.putBooleanVolatile(o, position, b);
    }

    public int pageSize() {
        return unsafe.pageSize();
    }

    public Object staticFieldBase(Field field) {
        return unsafe.staticFieldBase(field);
    }

    public int getLoadAverage(double[] doubles, int i) {
        return unsafe.getLoadAverage(doubles, i);
    }

    public void putDouble(Object o, long position, double v) {
        unsafe.putDouble(o, position, v);
    }

    public void putAddress(long position, long position2) {
        unsafe.putAddress(position, position2);
    }
}
