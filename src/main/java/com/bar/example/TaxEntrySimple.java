package com.bar.example;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class TaxEntrySimple {

    private String state;
    private String city;
    private int numEntries;
    private double price;

    public TaxEntrySimple(String state, String city, int numEntries, double price) {
        this.state = state;
        this.city = city;
        this.numEntries = numEntries;
        this.price = price;
    }

    record StateCityGroup(String state, String city) {
    }
    
    record TaxEntryAggregation(int totalNumEntries, double averagePrice) {
    }

    public String getState() {
        return state;
    }

    public String getCity() {
        return city;
    }

    public int getNumEntries() {
        return numEntries;
    }

    public double getPrice() {
        return price;
    }

    @Override
    public int hashCode() {
        return Objects.hash(city, numEntries);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TaxEntrySimple other = (TaxEntrySimple) obj;
        return Objects.equals(city, other.city) && numEntries == other.numEntries;
    }

    @Override
    public String toString() {
        return "TaxEntrySimple [state=" + state + ", city=" + city + ", numEntries=" + numEntries + ", price=" + price + "]";
    }
    
    public static void main(String[] params) {
  
        System.out.println("Starting TaxEntry aggregation");
        
      List<TaxEntrySimple> taxes = Arrays.asList(new TaxEntrySimple("New York", "NYC", 2, 20.0), 
                                              new TaxEntrySimple("New York", "NYC", 4, 10.0), 
                                              new TaxEntrySimple("New York", "NYC", 6, 10.4), 
                                              new TaxEntrySimple("Florida", "Orlando", 3, 13.3));
    
      Map<String, Integer> totalNumEntriesByCity = 
              taxes.stream().collect(Collectors.groupingBy(TaxEntrySimple::getCity, 
                                       Collectors.summingInt(TaxEntrySimple::getNumEntries)));
      
      int totalNumEntriesForNYC = totalNumEntriesByCity.get("NYC");
      
      Map<TaxEntry.StateCityGroup, Integer> totalNumEntriesByStateCity = 
              taxes.stream().collect(Collectors.groupingBy(
                                                 p -> new TaxEntry.StateCityGroup(p.getState(), p.getCity()), 
                                                 Collectors.summingInt(TaxEntrySimple::getNumEntries)));
      
      Map<StateCityGroup, TaxEntryAggregation> aggregationByStateCity = 
                  taxes.stream().collect(Collectors.groupingBy(p -> new StateCityGroup(p.getState(), p.getCity()),
                                                               Collectors.collectingAndThen(Collectors.toList(), list -> {
                                                                                            int entries = list.stream().collect(
                                                                                            Collectors.summingInt(TaxEntrySimple::getNumEntries));
                                                                                            double priceAverage = list.stream().collect(
                                                                                            Collectors.averagingDouble(TaxEntrySimple::getPrice));
                                                                                            return new TaxEntryAggregation(entries, priceAverage);})));
      double averagePriceForNY = aggregationByStateCity.get(new TaxEntrySimple.StateCityGroup("New York", "NYC")).averagePrice();
      System.out.println("Finished");
    }
}
