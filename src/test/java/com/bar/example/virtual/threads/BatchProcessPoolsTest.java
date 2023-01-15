package com.bar.example.virtual.threads;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Before;
import org.junit.Test;

public class BatchProcessPoolsTest {

	private static final double DELTA = 1e-10;
	public static int ARRAY_LENGTH = 10000;
	private double[] arr = new double[ARRAY_LENGTH];
	private double mean = 0.0;
	private ExecutorService fixSizePoolService = Executors.newFixedThreadPool(8);
	private ExecutorService cachedPoolService = Executors.newCachedThreadPool();
	
	@Before
	public void setup() {

		fixSizePoolService = Executors.newFixedThreadPool(8);
		cachedPoolService = Executors.newCachedThreadPool();
		double sum = 0.0;
		for(int i = 0; i < arr.length; i++) {
			
			arr[i] = (Math.random() * 100);
			sum = sum + arr[i];
		}
		mean =  sum / ARRAY_LENGTH;
	}
	
	@Test
	public void analyzeSensorDataCalculationFixedThreadPoolTest() throws InterruptedException, ExecutionException {
		
		assertEquals("Check array mean", mean, BatchProcessPools.analyzeSensorData(arr, fixSizePoolService), DELTA);
	}
	
	@Test
	public void analyzeSensorDataCalculationCachedThreadPoolTest() throws InterruptedException, ExecutionException {
		
		assertEquals("Check array mean", mean, BatchProcessPools.analyzeSensorData(arr, cachedPoolService), DELTA);
	}
}
