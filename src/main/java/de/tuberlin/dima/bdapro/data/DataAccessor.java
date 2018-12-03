package de.tuberlin.dima.bdapro.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Vector;

import de.tuberlin.dima.bdapro.error.BusinessException;
import de.tuberlin.dima.bdapro.model.TaxiRides;
import lombok.Getter;


public class DataAccessor {

	@Getter
	private int cursor = -1;
	@Getter
	private int length = 0;
	@Getter
	private TaxiRides taxiRides;
	
	@Getter
	private File file;
	
	public DataAccessor(File file) {
		this.file = file;
	}
	
	public void loadData() {
		clear();
		
		taxiRides = TaxiRides.builder()
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
				taxiRides.getVendorId().add(Integer.valueOf(values[i++]));
				i++;
//				taxiRides.getPickUpDate().add();
				i++;
//				taxiRides.getDropOffDate().add();
				taxiRides.getPassengerCount().add(Integer.valueOf(values[i++]));
				taxiRides.getDistance().add(Integer.valueOf(values[i++].replace(".", "")));
				taxiRides.getRating().add(Short.valueOf(values[i++]));
				taxiRides.getStoreFwdFlag().add(values[i++].charAt(0));
				taxiRides.getPickUpLocation().add(Integer.valueOf(values[i++]));
				taxiRides.getDropOffLocation().add(Integer.valueOf(values[i++]));
				taxiRides.getPaymentType().add(Integer.valueOf(values[i++]));
				taxiRides.getFare().add(Integer.valueOf(values[i++].replace(".", "")));
				taxiRides.getExtra().add(Integer.valueOf(values[i++].replace(".", "")));
				taxiRides.getMtaTax().add(Integer.valueOf(values[i++].replace(".", "")));
				taxiRides.getTip().add(Integer.valueOf(values[i++].replace(".", "")));
				taxiRides.getTolls().add(Integer.valueOf(values[i++].replace(".", "")));
				taxiRides.getImprovementSurcharge().add(Integer.valueOf(values[i++].replace(".", "")));
				taxiRides.getTotalAmount().add(Integer.valueOf(values[i].replace(".", "")));
			}

			length = taxiRides.getVendorId().size();
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
	}
	
	
	public Integer getVendorId() {
		return taxiRides.getVendorId().get(cursor);
	}
	
	public Integer getPassengerCount() {
		return taxiRides.getPassengerCount().get(cursor);
	}
	
	public Integer getDistance() {
		return taxiRides.getDistance().get(cursor);
	}
	
	public Short getRating() {
		return taxiRides.getRating().get(cursor);
	}
	
	public Character getStoreFwdFlag() {
		return taxiRides.getStoreFwdFlag().get(cursor);
	}
	
	public Integer getPickUpLocation() {
		return taxiRides.getPickUpLocation().get(cursor);
	}
	
	public Integer getDropOffLocation() {
		return taxiRides.getDropOffLocation().get(cursor);
	}
	
	public Integer getPaymentType() {
		return taxiRides.getPaymentType().get(cursor);
	}
	
	public Integer getFare() {
		return taxiRides.getFare().get(cursor);
	}
	
	public Integer getExtra() {
		return taxiRides.getExtra().get(cursor);
	}
	
	public Integer getMtaTax() {
		return taxiRides.getMtaTax().get(cursor);
	}
	
	public Integer getTip() {
		return taxiRides.getTip().get(cursor);
	}
	
	public Integer getTolls() {
		return taxiRides.getTolls().get(cursor);
	}
	
	public Integer getImprovementSurcharge() {
		return taxiRides.getImprovementSurcharge().get(cursor);
	}
	
	public Integer getTotalAmount() {
		return taxiRides.getTotalAmount().get(cursor);
	}
	
	public boolean hasNext() {
		return cursor < length - 1;
	}
	
	public synchronized boolean next() {
		cursor++;
		return hasNext();
	}
	
}
