package de.tuberlin.dima.bdapro.data;

import lombok.Setter;

public class TaxiRide {
	
	private final DataAccessor dataAccessor;
	@Setter
	private int cursor;
	
	
	public TaxiRide(DataAccessor dataAccessor, int cursor) {
		this.dataAccessor = dataAccessor;
		this.cursor = cursor;
	}
	
	
	public Integer getVendorId() {
		return this.dataAccessor.getTaxiRides().getVendorId().get(cursor);
	}
	
	
	public Integer getPassengerCount() {
		return this.dataAccessor.getTaxiRides().getPassengerCount().get(cursor);
	}
	
	
	public Integer getDistance() {
		return this.dataAccessor.getTaxiRides().getDistance().get(cursor);
	}
	
	
	public Short getRating() {
		return this.dataAccessor.getTaxiRides().getRating().get(cursor);
	}
	
	
	public Character getStoreFwdFlag() {
		return this.dataAccessor.getTaxiRides().getStoreFwdFlag().get(cursor);
	}
	
	
	public Integer getPickUpLocation() {
		return this.dataAccessor.getTaxiRides().getPickUpLocation().get(cursor);
	}
	
	
	public Integer getDropOffLocation() {
		return this.dataAccessor.getTaxiRides().getDropOffLocation().get(cursor);
	}
	
	
	public Integer getPaymentType() {
		return this.dataAccessor.getTaxiRides().getPaymentType().get(cursor);
	}
	
	
	public Integer getFare() {
		return this.dataAccessor.getTaxiRides().getFare().get(cursor);
	}
	
	
	public Integer getExtra() {
		return this.dataAccessor.getTaxiRides().getExtra().get(cursor);
	}
	
	
	public Integer getMtaTax() {
		return this.dataAccessor.getTaxiRides().getMtaTax().get(cursor);
	}
	
	
	public Integer getTip() {
		return this.dataAccessor.getTaxiRides().getTip().get(cursor);
	}
	
	
	public Integer getTolls() {
		return this.dataAccessor.getTaxiRides().getTolls().get(cursor);
	}
	
	
	public Integer getImprovementSurcharge() {
		return this.dataAccessor.getTaxiRides().getImprovementSurcharge().get(cursor);
	}
	
	
	public Integer getTotalAmount() {
		return this.dataAccessor.getTaxiRides().getTotalAmount().get(cursor);
	}
	
}
