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

import jdk.incubator.concurrent.StructuredTaskScope;

public class BatchProcess {

	record InputEntry(String url, String id, String startTime, String endTime) {
	}

	public static void main(String[] params) throws IOException {

		System.out.println("Starting Batch Process");
		List<InputEntry> inputEntries = readRecordEntriesFromCSVFile();
		processData(inputEntries);
	}

	@SuppressWarnings("preview")
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

	public static String processSensorData(InputEntry inputEntry) throws IOException, InterruptedException {

		DoubleStream data = fetchSensorData(inputEntry);

		return "ID: " + inputEntry.id() + ": " + analyzeSensorData(data);
	}

	private static DoubleStream fetchSensorData(InputEntry inputEntry)
			throws MalformedURLException, InterruptedException {
		URL pwUrl = new URL(inputEntry.url() + "/startTime/endTime");
		// In a real application open a secure url stream and fetch the data
		// For this example we return some random data and simulate network latencies
		Thread.sleep((long) Math.random());
		DoubleStream data = DoubleStream.generate(() -> new Random().nextDouble()).limit(100);
		return data;
	}

	// Do data analysis to check if system is working properly or has issues
	public static int analyzeSensorData(DoubleStream data) {

		double resultMean = 0;
		// Does parallel processing to improve performance using platform threads with
		// all available multi-core processors
		// In real application do proper data analysis,
		resultMean = data.parallel().average().getAsDouble();
		return determineStatusCode(resultMean);
	}

	public static int determineStatusCode(double result) {
		// In real application determine code based on result parameters
		if (result > 0.49) {
			return 719; // Made up error code
		} else {
			return 0;
		}
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

	public static int validateAndAnalyzeSensorData(DoubleStream data) throws IOException, InterruptedException {

		try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {

		    Future<String> validatedData = scope.fork(() -> validateData(data));
		    Future<String> checkedEnvironment = scope.fork(() -> checkEnvironment(data));
		    scope.join();
		    scope.throwIfFailed(e -> new IOException(e));
		    return analyzeSensorData(data);
		}
	}
	
	public static String validateData(DoubleStream data)
	{
		
		return "";
	}

	public static String checkEnvironment(DoubleStream data) {

		return "";
	}
}