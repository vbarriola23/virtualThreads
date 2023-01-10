package com.bar.example;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.reducing;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.stream.Collectors;


public class TaxEntry {
    private String state;
    private String city;
    private BigDecimal rate;
    private BigDecimal price;

    public TaxEntry(String state, String city, BigDecimal rate, BigDecimal price) {
        this.state = state;
        this.city = city;
        this.rate = rate;
        this.price = price;
    }

    record StateCityGroup(String state, String city) {
    }
    
    record RatePriceAggregation(int count, BigDecimal ratePrice) {
    }

    record TaxEntryAggregation(int count, BigDecimal weightedAveragePrice, 
                                BigDecimal totalPrice) {
    }

    public String getCity() {
        return this.city;
    }

    public String getState() {
        return this.state;
    }

    public void setRate(BigDecimal rate) {
        this.rate = rate;
    }

    public BigDecimal getRate() {
        return this.rate;
    }

    public BigDecimal getPrice() {
        return this.price;
    }

    public void setPrice(BigDecimal pr) {
        this.price = pr;
    }

    @Override
    public int hashCode() {
        return Objects.hash(city, state);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TaxEntry other = (TaxEntry) obj;
        return Objects.equals(city, other.city) && Objects.equals(state, other.state);
    }

    @Override
    public String toString() {
        return "TaxEntry [city=" + city + ", state=" + state + ", rate=" + rate + ", price=" + price + "]";
    }

    public static void main(String[] params) {
        System.out.println("Starting TaxEntry aggregation");
        
        List<TaxEntry> taxes = Arrays.asList(new TaxEntry("New York", "NYC", BigDecimal.valueOf(0.2), BigDecimal.valueOf(20.0)), 
                                                new TaxEntry("New York", "NYC", BigDecimal.valueOf(0.4), BigDecimal.valueOf(10.0)), 
                                                new TaxEntry("New York", "NYC", BigDecimal.valueOf(0.6), BigDecimal.valueOf(10.0)), 
                                                new TaxEntry("Florida", "Orlando", BigDecimal.valueOf(0.3), BigDecimal.valueOf(13.0)));
//        List<TaxEntry> taxes = new ArrayList<>();
//        Random random = new Random();
//        for (int i = 0; i < 1000000; i++) {
//
//            TaxEntry entry = new TaxEntry(generateString(), generateString(), BigDecimal.valueOf(random.nextInt(100)), BigDecimal.valueOf(random.nextInt(100)));
//            taxes.add(entry);
//        } 
//        taxes.add(new TaxEntry("New York", "NYC", BigDecimal.valueOf(0.3), BigDecimal.valueOf(20.0)));
//        taxes.add(new TaxEntry("New York", "NYC", BigDecimal.valueOf(0.7), BigDecimal.valueOf(10.0)));
//        taxes.add(new TaxEntry("New York", "NYC", BigDecimal.valueOf(0.6), BigDecimal.valueOf(10.0)));
 
//        long start = System.currentTimeMillis();
//        System.out.println("Starting process mapTo"); 
        
        Map<StateCityGroup, RatePriceAggregation> mapAggregation = taxes.stream().collect(
                Collectors.toMap(p -> new StateCityGroup(p.getState(), p.getCity()), 
                                 p -> new RatePriceAggregation(1, p.getRate().multiply(p.getPrice())), 
                                 (u1,u2) -> new RatePriceAggregation(u1.count() + u2.count(), u1.ratePrice().add(u2.ratePrice()))
                                 ));
        
        System.out.println("Map aggregation: " + mapAggregation.get(new StateCityGroup("New York", "NYC")));
                               
//        long end = System.currentTimeMillis();
//        System.out.println("process duration: " + (end - start) + " miliseconds");
//        System.out.println("Number of aggregations: " + mapAggregation.size());
//        System.out.println("Finished aggregation: " + mapAggregation.get(new StateCityGroup("New York", "NYC")));
        
        long start = System.currentTimeMillis();
        System.out.println("Starting process groupBy");       
        Map<StateCityGroup, TaxEntryAggregation> groupByAggregation = taxes.stream().collect(
                groupingBy(p -> new StateCityGroup(p.getState(), p.getCity()), 
                           mapping(p -> new TaxEntryAggregation(1, p.getRate().multiply(p.getPrice()), p.getPrice()), 
                                   collectingAndThen(reducing(new TaxEntryAggregation(0, BigDecimal.ZERO, BigDecimal.ZERO),
                                                              (u1,u2) -> new TaxEntryAggregation(u1.count() + u2.count(),
                                                                                         u1.weightedAveragePrice().add(u2.weightedAveragePrice()), 
                                                                                         u1.totalPrice().add(u2.totalPrice()))
                                                              ),
                                                     u -> new TaxEntryAggregation(u.count(), 
                                                                u.weightedAveragePrice().divide(BigDecimal.valueOf(u.count()), 2, RoundingMode.HALF_DOWN), 
                                                                u.totalPrice())
                                                     )
                                  )
                           ));      
        long end = System.currentTimeMillis();
        System.out.println("process duration: " + (end - start) + " miliseconds");
        System.out.println("Number of aggregations: " + groupByAggregation.size());
        System.out.println("Finished aggregation: " + groupByAggregation.get(new StateCityGroup("New York", "NYC")));                                
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

}