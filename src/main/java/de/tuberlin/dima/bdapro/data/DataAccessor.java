package de.tuberlin.dima.bdapro.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Vector;
import java.util.stream.Stream;

import de.tuberlin.dima.bdapro.error.BusinessException;
import de.tuberlin.dima.bdapro.model.DataStore;
import lombok.Getter;


public class DataAccessor {
	
	@Getter
	private int cursor = -1;
	@Getter
	private int length = 0;
	@Getter
	private DataStore dataStore;
	
	@Getter
	private File file;
	
	
	public DataAccessor(File file) {
		this.file = file;
	}
	
	
	public void loadData() {
		clear();
		
		dataStore = DataStore.builder()
				.vendorId(new Vector<>())
				.distance(new Vector<>())
				.pickUpDate(new Vector<>())
				.dropOffDate(new Vector<>())
				.passengerCount(new Vector<>())
				.rating(new Vector<>())
				.storeFwdFlag(new Vector<>())
				.pickUpLocation(new Vector<>())
				.dropOffLocation(new Vector<>())
				.paymentType(new Vector<>())
				.fare(new Vector<>())
				.extra(new Vector<>())
				.mtaTax(new Vector<>())
				.tip(new Vector<>())
				.tolls(new Vector<>())
				.improvementSurcharge(new Vector<>())
				.totalAmount(new Vector<>())
				.build();
		
		try (BufferedReader br = new BufferedReader(new FileReader(file))) {
			String line = "", separator = ",";

//			skip the first two lines (header line and empty line)
			br.readLine();
			br.readLine();
			
			while ((line = br.readLine()) != null) {
				String[] values = line.split(separator);
				
				int i = 0;
				dataStore.getVendorId().add(Integer.valueOf(values[i++]));
				i++;
//				dataStore.getPickUpDate().add();
				i++;
//				dataStore.getDropOffDate().add();
				dataStore.getPassengerCount().add(Integer.valueOf(values[i++]));
				dataStore.getDistance().add((int) (Double.valueOf(values[i++]) * 100));
				dataStore.getRating().add(Short.valueOf(values[i++]));
				dataStore.getStoreFwdFlag().add(values[i++].charAt(0));
				dataStore.getPickUpLocation().add(Integer.valueOf(values[i++]));
				dataStore.getDropOffLocation().add(Integer.valueOf(values[i++]));
				dataStore.getPaymentType().add(Integer.valueOf(values[i++]));
				dataStore.getFare().add((int) (Double.valueOf(values[i++]) * 100));
				dataStore.getExtra().add((int) (Double.valueOf(values[i++]) * 100));
				dataStore.getMtaTax().add((int) (Double.valueOf(values[i++]) * 100));
				dataStore.getTip().add((int) (Double.valueOf(values[i++]) * 100));
				dataStore.getTolls().add((int) (Double.valueOf(values[i++]) * 100));
				dataStore.getImprovementSurcharge().add((int) (Double.valueOf(values[i++]) * 100));
				dataStore.getTotalAmount().add((int) (Double.valueOf(values[i]) * 100));
			}
			
			length = dataStore.getVendorId().size();
		} catch (FileNotFoundException e) {
			throw new BusinessException("File [" + file.getName() + "] not found: " + e.getMessage(), e);
		} catch (IOException e) {
			throw new BusinessException("Could not read file [" + file.getName() + "]: " + e.getMessage(), e);
		}
	}
	
	
	public void reset() {
		cursor = -1;
	}
	
	
	private void clear() {
		cursor = -1;
		length = 0;
		dataStore = null;
	}


	public Integer getVendorId() {
		return dataStore.getVendorId().get(cursor);
	}

	public Integer getPassengerCount() {
		return dataStore.getPassengerCount().get(cursor);
	}

	public Integer getDistance() {
		return dataStore.getDistance().get(cursor);
	}

	public Short getRating() {
		return dataStore.getRating().get(cursor);
	}

	public Character getStoreFwdFlag() {
		return dataStore.getStoreFwdFlag().get(cursor);
	}

	public Integer getPickUpLocation() {
		return dataStore.getPickUpLocation().get(cursor);
	}

	public Integer getDropOffLocation() {
		return dataStore.getDropOffLocation().get(cursor);
	}

	public Integer getPaymentType() {
		return dataStore.getPaymentType().get(cursor);
	}

	public Integer getFare() {
		return dataStore.getFare().get(cursor);
	}

	public Integer getExtra() {
		return dataStore.getExtra().get(cursor);
	}

	public Integer getMtaTax() {
		return dataStore.getMtaTax().get(cursor);
	}

	public Integer getTip() {
		return dataStore.getTip().get(cursor);
	}

	public Integer getTolls() {
		return dataStore.getTolls().get(cursor);
	}

	public Integer getImprovementSurcharge() {
		return dataStore.getImprovementSurcharge().get(cursor);
	}

	public Integer getTotalAmount() {
		return dataStore.getTotalAmount().get(cursor);
	}
	
	
	public boolean hasNext() {
		return cursor < length - 1;
	}
	
	
	public synchronized boolean next() {
		cursor++;
		return hasNext();
	}
	
	
	public Stream<TaxiRide> stream() {
		reset();
		
		if (!hasNext()) {
			return Stream.empty();
		}
		

//		return Stream
//				.generate(() -> {
//					next();
//					return new TaxiRide(this, cursor);
//				})
////				.parallel()
//				.limit(length);

		
		next();
		return Stream
				.iterate(new TaxiRide(this, cursor), taxiRide -> {
					this.next();
					taxiRide.setCursor(cursor);
					return taxiRide;
//					return new TaxiRide(this, cursor);
				})
				.limit(length);
	}
	
}
