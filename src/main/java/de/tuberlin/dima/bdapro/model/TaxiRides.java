package de.tuberlin.dima.bdapro.model;

import java.util.Vector;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class TaxiRides {
	
	private Vector<Integer>   vendorId;
	private Vector<Long> 	  pickUpDate;
	private Vector<Long> 	  dropOffDate;
	private Vector<Integer>   passengerCount;
	private Vector<Integer>   distance;
	private Vector<Short> 	  rating;
	private Vector<Character> storeFwdFlag;
	private Vector<Integer>   pickUpLocation;
	private Vector<Integer>   dropOffLocation;
	private Vector<Integer>   paymentType;
	private Vector<Integer>   fare;
	private Vector<Integer>   extra;
	private Vector<Integer>   mtaTax;
	private Vector<Integer>   tip;
	private Vector<Integer>   tolls;
	private Vector<Integer>   improvementSurcharge;
	private Vector<Integer>   totalAmount;
	
}
