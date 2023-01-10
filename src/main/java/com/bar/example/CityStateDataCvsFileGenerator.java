package com.bar.example;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

public class CityStateDataCvsFileGenerator {

	public Map<Integer, String[]> townsStateMap = new HashMap<>();

	public static void main(String[] args) throws IOException {

		CityStateDataCvsFileGenerator csg = new CityStateDataCvsFileGenerator();
		csg.readTownsStatesCSVFile();
		csg.generateCsvFile();
	}

	public void generateCsvFile() throws IOException {

		String[] HEADERS = { "city", "state", "basePrice", "actualPrice" };
		Writer out = new FileWriter("C:\\Users\\mbarr\\Documents\\recordEntries10.csv");
		Random rd = new Random();
		try (CSVPrinter printer = new CSVPrinter(out, CSVFormat.DEFAULT.withHeader(HEADERS))) {

			for (int i = 0; i < 10000000; i++) {

				int index = rd.nextInt(townsStateMap.size());
				String[] townState = townsStateMap.get(index);
				if (townState != null)
					printer.printRecord(townState[0], townState[1], String.format("%.2f", rd.nextDouble() * 100), String.format("%.2f", rd.nextDouble() * 1000));
			}
		}
	}

	public void readTownsStatesCSVFile() throws IOException {

		try (Reader in = new FileReader("C:\\Users\\mbarr\\Documents\\CityStateUSA.csv");
				CSVParser records = CSVFormat.DEFAULT.withHeader().withDelimiter('|').withFirstRecordAsHeader().parse(in);) {
			
			int index = 1;
			for (CSVRecord record : records) {
				String[] cityState = new String[2];
				try {
					cityState[0] = record.get(4);
					cityState[1] = record.get(1);
				} catch (IndexOutOfBoundsException ex) {
					continue;
				}
				townsStateMap.put(index, cityState);
				index++;
			}
		}
	}
}
