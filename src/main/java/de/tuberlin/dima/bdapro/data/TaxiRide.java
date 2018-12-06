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
		return this.dataAccessor.getDataStore().getVendorId().get(cursor);
	}
	
	
	public Integer getPassengerCount() {
		return this.dataAccessor.getDataStore().getPassengerCount().get(cursor);
	}
	
	
	public Integer getDistance() {
		return this.dataAccessor.getDataStore().getDistance().get(cursor);
	}
	
	
	public Short getRating() {
		return this.dataAccessor.getDataStore().getRating().get(cursor);
	}
	
	
	public Character getStoreFwdFlag() {
		return this.dataAccessor.getDataStore().getStoreFwdFlag().get(cursor);
	}
	
	
	public Integer getPickUpLocation() {
		return this.dataAccessor.getDataStore().getPickUpLocation().get(cursor);
	}
	
	
	public Integer getDropOffLocation() {
		return this.dataAccessor.getDataStore().getDropOffLocation().get(cursor);
	}
	
	
	public Integer getPaymentType() {
		return this.dataAccessor.getDataStore().getPaymentType().get(cursor);
	}
	
	
	public Integer getFare() {
		return this.dataAccessor.getDataStore().getFare().get(cursor);
	}
	
	
	public Integer getExtra() {
		return this.dataAccessor.getDataStore().getExtra().get(cursor);
	}
	
	
	public Integer getMtaTax() {
		return this.dataAccessor.getDataStore().getMtaTax().get(cursor);
	}
	
	
	public Integer getTip() {
		return this.dataAccessor.getDataStore().getTip().get(cursor);
	}
	
	
	public Integer getTolls() {
		return this.dataAccessor.getDataStore().getTolls().get(cursor);
	}
	
	
	public Integer getImprovementSurcharge() {
		return this.dataAccessor.getDataStore().getImprovementSurcharge().get(cursor);
	}
	
	
	public Integer getTotalAmount() {
		return this.dataAccessor.getDataStore().getTotalAmount().get(cursor);
	}
	
}
