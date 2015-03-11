package com.sovrn.interview.mr;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import com.sovrn.interview.mr.AverageScore;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

public class AverageScoreTest {
	private final String outputPath = "junit-output";
	AverageScore score;
	
	@Before
	public void clean() {
		try {
			FileUtils.deleteDirectory(new File(outputPath));
		} catch (IOException e) {
			// ignore
		}
	}
	
	private Map<String, Float> getOutput() {
		Scanner scanner = null;
		Map<String, Float> results = new HashMap<String, Float>();
		
		try {
			scanner = new Scanner(new File(outputPath+"/part-r-00000"));
			while(scanner.hasNext()) {
				String values[] = scanner.nextLine().split("\t");
				if(values.length < 2)
					fail("Output contained invalid line");
				results.put(values[0], Float.parseFloat(values[1]));
			}
		} catch (FileNotFoundException e) {
			fail("Output file not found: "+e);
		} catch (NumberFormatException e) {
			fail("Output file contained data without a numerical value: "+e);
		} finally {
			if(scanner != null)
			    scanner.close();
		}
		return results;
	}
	
	@Test
	public void mapReduceBasicTest() {
		try {
			int retVal = AverageScore.mapReduce("src/main/data/data.tsv", outputPath);
			assertEquals(0,retVal);
			
			// Verify basic output format and number of entries
			assertEquals(189, getOutput().size());
		} catch (Exception e) {
			fail("Exceptions should not be thrown, but threw "+e);
		}
	}
	
	@Test
	public void mapReduceSpecificTest() {
		Map<String, Float> expectedOutput = new HashMap<String, Float>();
		expectedOutput.put("411mania.com", 0.5305795f); // Manually verified average
		expectedOutput.put("starcasm.net", 0.897649f);
		expectedOutput.put("totalfratmove.com", 0.506808f);
		
		try {
			// Note that data contains an invalid line, an invalid domain, and a line without a number.
			int retVal = AverageScore.mapReduce("src/main/data/junit-data.tsv", outputPath);
			assertEquals(0,retVal);
			
			// Verify output exactly
			Map<String, Float> output = getOutput();
			assertEquals(3, output.size());
			for(Map.Entry<String, Float> entry : output.entrySet()) {
				assertTrue(expectedOutput.keySet().contains(entry.getKey()));
				assertEquals(expectedOutput.get(entry.getKey()), entry.getValue());
			}
		} catch (Exception e) {
			fail("Exceptions should not be thrown, but threw "+e);
		}
	}

}
