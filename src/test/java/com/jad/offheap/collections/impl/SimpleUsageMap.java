package com.jad.offheap.collections.impl;

import com.jad.offheap.collections.impl.OffHeapHashMap;
import com.jad.offheap.collections.serializer.Serializers;
import org.apache.commons.lang.RandomStringUtils;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.util.Map;

/**
 * @author: Ilya Krokhmalyov YC14IK1
 * @since: 12/15/13
 */

public class SimpleUsageMap {

    @Test
    public void simpleTest() {
        Map<Integer, People> map = new OffHeapHashMap<Integer, People>(1000, 0.75f
                , Serializers.INT_SERIALIZER
                , new Serializers.SerializableSerializer<People>());

        long start = System.currentTimeMillis();
        for (int index = 0; index < 1000; index++) {
            map.put(index, People.randomPeople());
        }
        System.out.println("Time:" + (System.currentTimeMillis() - start));
        System.out.println(map);
        map.clear();
    }

    private static class People implements Serializable {

        private String name;

        private String surname;

        private int age;

        public static People randomPeople() {
            People people = new People();
            people.setName(RandomStringUtils.randomAlphabetic(8));
            people.setSurname(RandomStringUtils.randomAlphabetic(10));
            people.setAge((int) (Math.random() * 100));
            return people;
        }

        private People() {
        }

        private int getAge() {
            return age;
        }

        private void setAge(int age) {
            this.age = age;
        }

        private String getName() {
            return name;
        }

        private void setName(String name) {
            this.name = name;
        }

        private String getSurname() {
            return surname;
        }

        private void setSurname(String surname) {
            this.surname = surname;
        }

        @Override
        public String toString() {
            return "People{" +
                    "name='" + name + '\'' +
                    ", surname='" + surname + '\'' +
                    ", age=" + age +
                    '}';
        }
    }
}
