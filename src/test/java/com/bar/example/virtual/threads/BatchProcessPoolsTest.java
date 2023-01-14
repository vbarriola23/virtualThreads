package com.bar.example.virtual.threads;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.ExecutionException;

import org.junit.Before;
import org.junit.Test;

public class BatchProcessPoolsTest {

	private static final double DELTA = 1e-10;
	public static int ARRAY_LENGTH = 10000;
	private double[] arr = new double[ARRAY_LENGTH];
	private double mean = 0.0;
	
	@Before
	public void setup() {
		
		double sum = 0.0;
		for(int i = 0; i < arr.length; i++) {
			
			arr[i] = (Math.random() * 100);
			sum = sum + arr[i];
		}
		mean =  sum / ARRAY_LENGTH;
	}
	
	@Test
	public void analyzeSensorDataCalculationTest() throws InterruptedException, ExecutionException {
		
		assertEquals("Check array mean", mean, BatchProcessPools.analyzeSensorData(arr), DELTA);
	}
}
