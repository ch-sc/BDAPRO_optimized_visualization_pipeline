package de.tuberlin.dima.bdapro.data.taxi;

import java.io.File;

import de.tuberlin.dima.bdapro.config.DataConfig;
import de.tuberlin.dima.bdapro.config.AppConfigLoader;
import de.tuberlin.dima.bdapro.data.DataProcessor;
import de.tuberlin.dima.bdapro.data.GenericDataAccessor;
import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@Slf4j
public class SimpleDataProcessorTest {
	
	private static DataProcessor dataProcessor;
	private static GenericDataAccessor<TaxiRide> dataAccessor;

	static {
//		init();
	}
	
	private static TaxiRide loadTaxiData(DataConfig config) {
		dataAccessor = new GenericDataAccessor<>(new File(config.getDataLocation()));
		dataAccessor.loadData(TaxiRide::loadData);
		return new TaxiRide(dataAccessor, dataAccessor.getCursor() + 0, dataAccessor.getLength());
	}
	
	
	private static void init() {
		DataConfig config = AppConfigLoader.load(null);
		dataProcessor = new SimpleDataProcessor(config, loadTaxiData(config));
	}
	
	
	private static void logResult(int[][] scatter, int x, int y) {
		StringBuilder stringBuffer = new StringBuilder(100 * 100 * 2).append("Output:\n");
		for (int i = 0; i < Math.min(100, x - 1); i++) {
			for (int j = 0; j < Math.min(100, y - 1); j++) {
				stringBuffer.append(scatter[i][j]).append(" ");
			}
			stringBuffer.append("\n");
		}
		log.info(stringBuffer.toString());
	}
	
	@Before
	public void setUp() throws Exception {
		dataAccessor.reset();
	}
	
	
	@Test
	public void scatterPlot_X1000_Y1000() {
//		Arrange
		int x = 1000, y = 1000;
		int[][] scatter;
		
//		Act
		scatter = dataProcessor.scatterPlot(x, y);
		
//		Assert
		assertNotNull(scatter);
		assertEquals(scatter.length, x);
		assertEquals(scatter[0].length, y);

//		Print
		logResult(scatter, x, y);

		long counter = 0;
		for (int[] row : scatter) {
			for (int cell : row) {
				counter += cell;
			}
		}
		
		assertTrue(counter > 0);
	}
	
	@Test
	public void scatterPlot_X100_Y100() {
//		Arrange
		int x = 100, y = 100;
		int[][] scatter;

//		Act
		scatter = dataProcessor.scatterPlot(x, y);

//		Assert
		assertNotNull(scatter);
		assertEquals(scatter.length, x);
		assertEquals(scatter[0].length, y);

//		Print
		logResult(scatter, x, y);
		
		long counter = 0;
		for (int[] row : scatter) {
			for (int cell : row) {
				counter += cell;
			}
		}
		
		assertTrue(counter > 0);
	}
	
	
	@Test
	public void scatterPlot_X10_Y10() {
//		Arrange
		int x = 10, y = 10;
		int[][] scatter;

//		Act
		scatter = dataProcessor.scatterPlot(x, y);

//		Assert
		assertNotNull(scatter);
		assertEquals(scatter.length, x);
		assertEquals(scatter[0].length, y);

//		Print
		logResult(scatter, x, y);
		
		long counter = 0;
		for (int[] row : scatter) {
			for (int cell : row) {
				counter += cell;
			}
		}
		
		assertTrue(counter > 0);
	}
	
	@Test
	public void scatterPlot1() {
	}
}