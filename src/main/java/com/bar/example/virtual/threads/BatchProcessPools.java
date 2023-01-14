package com.bar.example.virtual.threads;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.DoubleStream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import com.bar.example.virtual.threads.BatchProcess.InputEntry;

import jdk.incubator.concurrent.StructuredTaskScope;

public class BatchProcessPools {
	
	//Determine number of threads based on system resource
	private static int NUM_THREADS = 8;
	private static ExecutorService fixSizePoolService = Executors.newFixedThreadPool(NUM_THREADS);

	record InputEntry(String url, String id, String startTime, String endTime) {
	}

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		System.out.println("Starting Batch Process");

		long[] inputEntries = {1,2,3,4,5,6};
		long mean = analyzeSensorData(inputEntries);
		System.out.println(mean);
//		try {
//
//			inputEntries = readRecordEntriesFromCSVFile();
//			processData(inputEntries);
//		} catch (IOException ex) {
//
//			System.out.println("Failed to read CSV file:" + ex.getMessage());
//		}
	}

	public static void processData(List<InputEntry> inputEntries) {

		System.out.println("processSensors()");
		ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
		CompletionService<String> cService = new ExecutorCompletionService<>(executor);

		for (InputEntry inputEntry : inputEntries) {

			cService.submit(() -> processSensorData(inputEntry));
		}

		int processed = 0;
		while (processed < inputEntries.size()) {
			processed++;
			try {
				Future<String> resultFuture = cService.take();
				System.out.println("Handle status:" + resultFuture.get());
			} catch (ExecutionException | InterruptedException e) {
				System.out.println("Failed to process:" + e.getMessage());
			}
		}
	}

	public static String processSensorData(InputEntry inputEntry)
			throws IOException, InterruptedException, ExecutionException {

		long[] data = fetchSensorData(inputEntry);

		return "ID: " + inputEntry.id() + ": " + analyzeSensorData(data);
	}

	private static long[] fetchSensorData(InputEntry inputEntry) throws MalformedURLException, InterruptedException {
		URL pwUrl = new URL(inputEntry.url() + "/startTime/endTime");
		// In a real application open a secure url stream and fetch the data
		// For this example we return some random data and simulate network latencies
		Thread.sleep((long) (Math.random() * 100));
		long[] data = new long[1000];
		// DoubleStream data = DoubleStream.generate(() -> new
		// Random().nextDouble()).limit(100);
		for (int i = 0; i < 1000; i++) {
			data[i] = (long) Math.random() * 100;
		}
		return data;
	}

	public static List<InputEntry> readRecordEntriesFromCSVFile() throws IOException {

		List<InputEntry> inputEntries = new ArrayList<>();

		try (Reader in = new FileReader("C:\\Users\\mbarr\\Documents\\recordEntriesPowerPlants.csv");
				CSVParser records = CSVFormat.DEFAULT.withHeader().withFirstRecordAsHeader().parse(in);) {

			for (CSVRecord record : records) {

				InputEntry inputEntry = new InputEntry(record.get(0), record.get(1), record.get(2), record.get(3));
				inputEntries.add(inputEntry);
			}
		}
		return inputEntries;
	}

	// Do data analysis to check if system is working properly or has issues
	public static long analyzeSensorData(long[] data) throws InterruptedException, ExecutionException {
		
		int segmentSize = data.length / NUM_THREADS;
		List<Future<Long>> futures = new ArrayList<>(); 
		int numSegments = 0;
		while ( data.length < (numSegments * segmentSize)) {
			int start = numSegments * segmentSize;
			int end = (numSegments+1) * segmentSize;
			futures.add(fixSizePoolService.submit(() -> {
				long segmentSum = 0;
				for (int j = start; j < end; j++) {
					segmentSum += data[j];
				}
				return segmentSum;
			}));
			numSegments ++;
		}
			
		long totalSum = 0;
		for (Future<Long> future : futures) {
			totalSum += future.get();
		}
		
		//add the left over segment
		int lastElementProcessed = numSegments * segmentSize;
		for (int i = lastElementProcessed; i < data.length; i++) {
			totalSum += data[i];
		}
		return totalSum/data.length;
	}
}
