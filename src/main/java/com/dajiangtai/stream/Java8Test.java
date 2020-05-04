package com.dajiangtai.stream;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class Java8Test {

    public static void main(String[] args) {
        Map<Integer, String> map = new LinkedHashMap<>();
        map.put(10, "apple");
        map.put(20, "orange");
        map.put(30, "banana");
        map.put(40, "watermelon");
        map.put(50, "dragonfruit");
        map.put(60, "dragonfruit");
        map.put(70,"red");
//        System.out.println("\n1. Export Map Key to List...");
//        List<Integer> result = new ArrayList(map.keySet());
//        result.forEach(System.out::println);
//        System.out.println("\n2. Export Map Value to List...");
//        List<String> result2 = new ArrayList(map.values());
//        result2.forEach(System.out::println);

        List<Integer> collect = map.keySet().stream().collect(Collectors.toList());
        collect.forEach(System.out::println);
        List<String> collect1 = map.values()
                .parallelStream()
                .map(x -> x + "_1")
                .skip(2)
                .distinct()
                .filter(x -> !x.equalsIgnoreCase("apple"))
                .sorted()
                .limit(3)
//                .count()
//                .reduce((x,y)->"red"+x+y)
//                .min((x,y)->Integer.compare(Integer.parseInt(x),Integer.parseInt(y))
//                .anyMatch(x->x.equalsIgnoreCase("red_1"))
                .collect(Collectors.toList());
        collect1.forEach(System.out::println);

    }
}
