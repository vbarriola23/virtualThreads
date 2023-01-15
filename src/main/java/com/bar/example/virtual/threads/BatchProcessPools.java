package com.bar.example.virtual.threads;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class BatchProcessPools {

	// Determine number of threads based on system resource
	private static int NUM_THREADS = 8;
	private static ExecutorService fixSizePoolService = Executors.newFixedThreadPool(NUM_THREADS);
	private static ExecutorService cachedPoolService = Executors.newCachedThreadPool();

	record InputEntry(String url, String id, String startTime, String endTime) {
	}

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		System.out.println("Starting Batch Process");

		List<InputEntry> inputEntries = new ArrayList<>();
		try {
			inputEntries = readRecordEntriesFromCSVFile();
			if (args.length == 0 || args[0].equalsIgnoreCase("fixThreadPool")) {
				processData(inputEntries, fixSizePoolService);
			} else {
				processData(inputEntries, cachedPoolService);
			}
		} catch (IOException ex) {

			System.out.println("Failed to read CSV file:" + ex.getMessage());
		}
	}

	public static void processData(List<InputEntry> inputEntries, ExecutorService threadPool) {

		System.out.println("processSensors()");
		ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
		CompletionService<String> cService = new ExecutorCompletionService<>(executor);

		for (InputEntry inputEntry : inputEntries) {

			cService.submit(() -> processSensorData(inputEntry, threadPool));
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

	public static String processSensorData(InputEntry inputEntry, ExecutorService threadPool)
			throws IOException, InterruptedException, ExecutionException {

		double[] data = fetchSensorData(inputEntry);

		return "ID: " + inputEntry.id() + ": " + analyzeSensorData(data, threadPool);
	}

	private static double[] fetchSensorData(InputEntry inputEntry) throws MalformedURLException, InterruptedException {
		URL pwUrl = new URL(inputEntry.url() + "/startTime/endTime");
		// In a real application open a secure url stream and fetch the data
		// For this example we return some random data and simulate network latencies
		Thread.sleep((long) (Math.random() * 100));
		double[] data = new double[1000];
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
	public static double analyzeSensorData(double[] data, ExecutorService threadPool)
			throws InterruptedException, ExecutionException {
		
		int segmentSize = data.length / NUM_THREADS;
		List<Future<Double>> futures = new ArrayList<>();
		
		int index = 0;
		while (index < NUM_THREADS) {
			int start = index * segmentSize;
			int end = (index + 1) * segmentSize;
			futures.add(threadPool.submit(() -> {
				double segmentSum = 0;
				for (int j = start; j < end; j++) {
					segmentSum += data[j];
				}
				return segmentSum;
			}));
			index++;
		}
		double totalSum = 0;
		for (Future<Double> future : futures) {
			totalSum += future.get();
		}
		// add the left over segment
		int lastElementProcessed = NUM_THREADS * segmentSize;
		for (int i = lastElementProcessed; i < data.length; i++) {
			totalSum += data[i];
		}
		return totalSum / data.length;
	}
}
