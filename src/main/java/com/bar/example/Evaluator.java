package com.bar.example;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import static java.util.stream.Collectors.groupingBy;

public class Evaluator {

    public static record Event(String location, int price, String eventType, int eventRankId) {
    }

    public static int evaluateLocationProximity(String location1, String location2) {

        return Math.abs(location1.hashCode() - location2.hashCode());
    }

    public static int evaluateProvidersAffinity(String localEventType, int localPrice, String targetEvtType, int targetPrice) {

        if (localEventType.equalsIgnoreCase(targetEvtType))
            return Math.abs(localPrice - targetPrice);
        else
            return (100 + Math.abs(localPrice - targetPrice));
    }

    public static List<Event> getNearEvents(int number, List<Event> events, String targetLocation) {

        Map<Integer, Set<Event>> distanceEventsMap = events.stream()
            .collect(groupingBy(event -> evaluateLocationProximity(event.location(), targetLocation), TreeMap::new, Collectors.toCollection(() -> new TreeSet<>(Comparator.comparingInt(Event::eventRankId)))));
        return distanceEventsMap.values()
            .stream()
            .flatMap(Set::stream)
            .limit(number)
            .collect(Collectors.toList());
    }
    
    public static List<Event> getNearEventsFunctionalInterface(int number, List<Event> events, String targetLocation, BiFunction<String, String, Integer> locationProximityEvaluator) {

        Map<Integer, Set<Event>> distanceEventsMap = events.stream()
            .collect(groupingBy(event -> locationProximityEvaluator.apply(event.location(), targetLocation), TreeMap::new, Collectors.toCollection(() -> new TreeSet<>(Comparator.comparingInt(Event::eventRankId)))));
        return distanceEventsMap.values()
            .stream()
            .flatMap(Set::stream)
            .limit(number)
            .collect(Collectors.toList());
    }

    public static List<Event> getNearEventsParallel(int number, List<Event> events, String targetLocation) {

        Map<Integer, Set<Event>> distanceEventsMap = events.stream()
            .parallel()
            .collect(Collectors.groupingBy(event -> evaluateLocationProximity(event.location(), targetLocation), TreeMap::new, Collectors.toCollection(() -> new TreeSet<>(Comparator.comparingInt(Event::eventRankId)))));
        return distanceEventsMap.values()
            .stream()
            .flatMap(Set::stream)
            .limit(number)
            .collect(Collectors.toList());
    }

    public static List<Event> getNearEventsConcurrent(int number, List<Event> events, String targetLocation) {

        ConcurrentSkipListMap<Integer, Set<Event>> distanceEventsMap = events.stream()
            .parallel()
            .collect(Collectors.groupingByConcurrent(event -> evaluateLocationProximity(event.location(), targetLocation), 
            		ConcurrentSkipListMap::new, Collectors.toCollection(() -> new TreeSet<>(Comparator.comparingInt(Event::eventRankId)))));
        return distanceEventsMap.values()
            .stream()
            .flatMap(Set::stream)
            .limit(number)
            .collect(Collectors.toList());
    }

    private static String generateString() {

        Random random = new Random();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 3 + random.nextInt(6); i++) {

            char ch = (char) (random.nextInt(26) + 'a');
            sb.append(ch);
        }
        return sb.toString();
    }

    List<Event> getEventsByProximityAndAffinity(int number, List<Event> events, String targetLocation, String targetType, int targetPrice) {
        Map<Integer, Map<Integer, Set<Event>>> chosenEvents = events.stream()
          .collect(groupingBy(event -> evaluateLocationProximity(event.location(), targetLocation), 
                                         TreeMap::new,
                                         Collectors.groupingBy(event -> evaluateProvidersAffinity(event.eventType(), 
                                                               event.price(), targetType, targetPrice), 
                                                               TreeMap::new, 
                                                               Collectors.toCollection(() -> new TreeSet<>(Comparator.comparingInt(Event::eventRankId))))));

        return chosenEvents.values()
            .stream()
            .flatMap(map -> map.values()
                .stream()
                .flatMap(Set::stream))
            .limit(number)
            .collect(Collectors.toList());
    }

    public static void main(String[] params) {

        // List<Event> events = Arrays.asList(new Event("New York", 6, "Classic", 1), new Event("New York", 4, "Classic", 2), new Event("New York", 4, "Classic", 6), new Event("New York", 4, "Jazz", 3), new Event("Miami", 5, "Jazz", 9),
        // new Event("Orlando", 5, "Classic", 10), new Event("New York", 7, "Jazz", 4));

        List<Event> events = new ArrayList<>();
        Random random = new Random();
        int rank = 1;
        for (int i = 0; i < 10000000; i++) {

            Event event = new Event(generateString(), random.nextInt(79), generateString(), rank++);
            events.add(event);
        }

        long start = System.currentTimeMillis();
        System.out.println("Starting process serial");
        List<Event> bestEvents = getNearEvents(2, events, "New York");
        System.out.println(bestEvents.toString());
        long end = System.currentTimeMillis();
        System.out.println("process duration: " + (end - start) + " miliseconds");

        start = System.currentTimeMillis();
        System.out.println("Starting process parallel");
        bestEvents = getNearEventsParallel(2, events, "New York");
        System.out.println(bestEvents.toString());
        end = System.currentTimeMillis();
        System.out.println("process duration parallel: " + (end - start) + " miliseconds");

        start = System.currentTimeMillis();
        System.out.println("Starting process concurrent");
        bestEvents = getNearEventsConcurrent(2, events, "New York");
        System.out.println(bestEvents.toString());
        end = System.currentTimeMillis();
        System.out.println("process duration concurrent: " + (end - start) + " miliseconds");

        System.out.println("Running process functional interface");
        bestEvents = getNearEventsFunctionalInterface(2, events, "New York", (l1, l2) ->  Math.abs(l1.hashCode() - l2.hashCode()));
        System.out.println(bestEvents.toString());

    }
}
