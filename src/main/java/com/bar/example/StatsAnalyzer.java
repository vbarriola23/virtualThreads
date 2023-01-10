package com.bar.example;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import com.google.common.math.StatsAccumulator;

public class StatsAnalyzer {
    
    record InputEntry(String city, String state, double basePrice, double actualPrice) {}
	record CityState(String city, String state) {};   
    record StatsAggregation(StatsAccumulator basePrice, StatsAccumulator actualPrice) {}
    
    public static void main(String[] args) throws IOException {
    	
    	System.out.println("Starting StatsAnalyzer");
    	
    	List<InputEntry> inputEntries = readRecordEntriesFromCSVFile();
    	long startTime = System.currentTimeMillis();
    	Map<CityState, StatsAggregation> stateAggr = getStatsAnalytics(inputEntries);
    	long endTime = System.currentTimeMillis();   	
    	System.out.println("Time elapsed in milliseconds: " + (endTime - startTime));
    	
		printResults(stateAggr, "Blountsville", "IN");
    	
    	startTime = System.currentTimeMillis();
    	stateAggr = getStatsAnalyticsParallel(inputEntries);
    	endTime = System.currentTimeMillis();    	
    	System.out.println("Time elapsed Parallel in milliseconds: " + (endTime - startTime));
    	
		printResults(stateAggr, "Blountsville", "IN");
    	
    	startTime = System.currentTimeMillis();
    	stateAggr = getStatsAnalyticsParallelGroupByConcurrent(inputEntries);
    	endTime = System.currentTimeMillis();    
    	
    	try {
			Thread.sleep(100000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		printResults(stateAggr, "Blountsville", "IN");
    }

	private static void printResults(Map<CityState, StatsAggregation> stateAggr, String city, String state) {
		StatsAggregation aggreg = stateAggr.get(new CityState(city, state));
    	System.out.println(city + " : " + state);
    	System.out.println("basePrice.mean: " + aggreg.basePrice().mean());
    	System.out.println("basePrice.sampleVariance: " + aggreg.basePrice().sampleVariance());
    	System.out.println("basePrice.sampleStandardDeviation: " + aggreg.basePrice().sampleStandardDeviation());
    	System.out.println("basePrice.count: " + aggreg.basePrice().count());
    	System.out.println("basePrice.min: " + aggreg.basePrice.min());
    	System.out.println("basePrice.max: " + aggreg.basePrice.max());
    	System.out.println("actualPrice.mean: " + aggreg.actualPrice().mean());
    	System.out.println("actualPrice.sampleVariance: " + aggreg.actualPrice().sampleVariance());
    	System.out.println("actualPrice.sampleStandardDeviation: " + aggreg.actualPrice().sampleStandardDeviation());
    	System.out.println("actualPrice.count: " + aggreg.actualPrice().count());
    	System.out.println("actualPrice.min: " + aggreg.actualPrice.min());
    	System.out.println("actualPrice.max: " + aggreg.actualPrice.max());
    	System.out.println("Map size:" + stateAggr.size());
	}
    
    public static Map<CityState, StatsAggregation> getStatsAnalytics(List<InputEntry> entries){
    	
    	Map<CityState, StatsAggregation> stats = entries.stream().filter(i -> !(i.state().equals("MN") || i.state().equals("CA"))).collect(
    			Collectors.groupingBy(entry -> new CityState(entry.city(), entry.state()), Collectors.collectingAndThen(Collectors.toList(), 
    					list -> {StatsAccumulator sac = new StatsAccumulator();
    							 sac.addAll(list.stream().mapToDouble(InputEntry::basePrice));
    							 StatsAccumulator sas = new StatsAccumulator();
    							 sas.addAll(list.stream().mapToDouble(InputEntry::actualPrice));
    							 return new StatsAggregation(sac, sas);
    							})));
    	return stats;
    }
    
    public static Map<CityState, StatsAggregation> getStatsAnalyticsParallel(List<InputEntry> entries){
    	
    	Map<CityState, StatsAggregation> stats = entries.stream().filter(i -> !(i.state().equals("MN") || i.state().equals("CA"))).parallel().collect(
    			Collectors.groupingBy(entry -> new CityState(entry.city(), entry.state()), Collectors.collectingAndThen(Collectors.toList(), 
    					list -> {StatsAccumulator sac = new StatsAccumulator();
    							 sac.addAll(list.stream().mapToDouble(InputEntry::basePrice));
    							 StatsAccumulator sas = new StatsAccumulator();
    							 sas.addAll(list.stream().mapToDouble(InputEntry::actualPrice));
    							 return new StatsAggregation(sac, sas);
    							})));
    	return stats;
    }
    
    public static Map<CityState, StatsAggregation> getStatsAnalyticsParallelGroupByConcurrent(List<InputEntry> entries){
    	
    	Map<CityState, StatsAggregation> stats = entries.stream().filter(i -> !(i.state().equals("MN") || i.state().equals("CA"))).parallel().unordered().collect(
    			Collectors.groupingByConcurrent(entry -> new CityState(entry.city(), entry.state()), Collectors.collectingAndThen(Collectors.toList(), 
    					list -> {StatsAccumulator sac = new StatsAccumulator();
    							 sac.addAll(list.stream().mapToDouble(InputEntry::basePrice));
    							 StatsAccumulator sas = new StatsAccumulator();
    							 sas.addAll(list.stream().mapToDouble(InputEntry::actualPrice));
    							 return new StatsAggregation(sac, sas);
    							})));
    	return stats;
    }
    
	public static List<InputEntry> readRecordEntriesFromCSVFile() throws IOException {
		
		List<InputEntry> inputEntries = new ArrayList<>();

		try (Reader in = new FileReader("C:\\Users\\mbarr\\Documents\\recordEntries.csv");
				CSVParser records = CSVFormat.DEFAULT.withHeader().withFirstRecordAsHeader().parse(in);) {
			
			for (CSVRecord record : records) {
				
				InputEntry inputEntry = new InputEntry(record.get(0), record.get(1), Double.parseDouble(record.get(2)), Double.parseDouble(record.get(3)));
				inputEntries.add(inputEntry);
			}
		}
		return inputEntries;
	}

}
