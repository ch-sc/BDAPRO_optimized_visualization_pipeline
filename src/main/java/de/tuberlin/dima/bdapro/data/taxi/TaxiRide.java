package de.tuberlin.dima.bdapro.data.taxi;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Spliterator;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import de.tuberlin.dima.bdapro.data.GenericDataAccessor;
import de.tuberlin.dima.bdapro.data.IDataAccessor;
import de.tuberlin.dima.bdapro.data.Streamable;
import de.tuberlin.dima.bdapro.error.BusinessException;
import de.tuberlin.dima.bdapro.data.DataStore;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.ArrayUtils;

public class TaxiRide implements IDataAccessor, Streamable<TaxiRide> {
	
	private final GenericDataAccessor dataAccessor;
	@Getter
	private final int offset;
	private int partialCursor;
	private int partialLength;
	
	private static final UUID VENDOR_ID_KEY = UUID.randomUUID();
	private static final UUID PICK_UP_DATE_KEY = UUID.randomUUID();
	private static final UUID DROP_OFF_DATE_KEY = UUID.randomUUID();
	private static final UUID PASSENGER_KEY = UUID.randomUUID();
	private static final UUID DISTANCE_KEY = UUID.randomUUID();
	private static final UUID RATING_KEY = UUID.randomUUID();
	private static final UUID STORED_FWD_KEY = UUID.randomUUID();
	private static final UUID PICK_UP_LOCATION_KEY = UUID.randomUUID();
	private static final UUID DROP_OFF_LOCATION_KEY = UUID.randomUUID();
	private static final UUID PAYMENT_KEY = UUID.randomUUID();
	private static final UUID FARE_KEY = UUID.randomUUID();
	private static final UUID EXTRA_KEY = UUID.randomUUID();
	private static final UUID MTA_TAX_KEY = UUID.randomUUID();
	private static final UUID TIP_KEY = UUID.randomUUID();
	private static final UUID TOLLS_KEY = UUID.randomUUID();
	private static final UUID IMPRoVEMENT_SURCHARGE_KEY = UUID.randomUUID();
	private static final UUID TOTAL_AMOUNT_KEY = UUID.randomUUID();
	
	private int[] vendorId;
	private long[] pickUpDate;
	private long[] dropOffDate;
	private int[] passengerCount;
	private int[] distance;
	private short[] rating;
	private char[] storeFwdFlag;
	private int[] pickUpLocation;
	private int[] dropOffLocation;
	private int[] paymentType;
	private int[] fare;
	private int[] extra;
	private int[] mtaTax;
	private int[] tip;
	private int[] tolls;
	private int[] improvementSurcharge;
	private int[] totalAmount;
	
	
	public TaxiRide(GenericDataAccessor dataAccessor, int offset, int length) {
		this.dataAccessor = dataAccessor;
		this.offset = offset/* + dataAccessor.getCursor()*/;
		this.partialCursor = offset/* + dataAccessor.getCursor()*/;
		this.partialLength = length;
		setColumns();
	}
	
	
	/**
	 * @param file
	 * @param dataAccessor
	 * @return
	 */
	public static int loadData(File file, GenericDataAccessor dataAccessor) {
		
		List<Integer> vendorIds = new ArrayList<>(1_000_000);
		List<Integer> distances = new ArrayList<>(1_000_000);
		List<Long> pickUpDate = new ArrayList<>(1_000_000);
		List<Long> dropOffDates = new ArrayList<>(1_000_000);
		List<Integer> passengers = new ArrayList<>(1_000_000);
		List<Short> ratings = new ArrayList<>(1_000_000);
		List<Character> storeFwdFlags = new ArrayList<>(1_000_000);
		List<Integer> pickUpLocations = new ArrayList<>(1_000_000);
		List<Integer> dropOffLocations = new ArrayList<>(1_000_000);
		List<Integer> paymentTypes = new ArrayList<>(1_000_000);
		List<Integer> fares = new ArrayList<>(1_000_000);
		List<Integer> extras = new ArrayList<>(1_000_000);
		List<Integer> mtaTaxes = new ArrayList<>(1_000_000);
		List<Integer> tips = new ArrayList<>(1_000_000);
		List<Integer> tolls = new ArrayList<>(1_000_000);
		List<Integer> improvementSurcharges = new ArrayList<>(1_000_000);
		List<Integer> totalAmounts = new ArrayList<>(1_000_000);
		
		try (BufferedReader br = new BufferedReader(new FileReader(file))) {
			String line = "", separator = ",";

//			skip the first two lines (header line and empty line)
			br.readLine();
			br.readLine();
			
			while ((line = br.readLine()) != null) {
				String[] values = line.split(separator);
				
				int i = 0;
				vendorIds.add(Integer.valueOf(values[i++]));
				// example date time format: 2017-12-01 00:12:00
				pickUpDate.add(LocalDateTime.parse(values[i++],
						DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")).toInstant(ZoneOffset.UTC).toEpochMilli());
				dropOffDates.add(LocalDateTime.parse(values[i++],
						DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")).toInstant(ZoneOffset.UTC).toEpochMilli());
				passengers.add(Integer.valueOf(values[i++]));
				distances.add((int) (Double.valueOf(values[i++]) * 100));
				ratings.add(Short.valueOf(values[i++]));
				storeFwdFlags.add(values[i++].charAt(0));
				pickUpLocations.add(Integer.valueOf(values[i++]));
				dropOffLocations.add(Integer.valueOf(values[i++]));
				paymentTypes.add(Integer.valueOf(values[i++]));
				fares.add((int) (Double.valueOf(values[i++]) * 100));
				extras.add((int) (Double.valueOf(values[i++]) * 100));
				mtaTaxes.add((int) (Double.valueOf(values[i++]) * 100));
				tips.add((int) (Double.valueOf(values[i++]) * 100));
				tolls.add((int) (Double.valueOf(values[i++]) * 100));
				improvementSurcharges.add((int) (Double.valueOf(values[i++]) * 100));
				totalAmounts.add((int) (Double.valueOf(values[i]) * 100));
			}
			
			DataStore dataStore = dataAccessor.getDataStore();
			
			dataStore.putColumn(VENDOR_ID_KEY, ArrayUtils.toPrimitive(vendorIds.toArray(new Integer[vendorIds.size()])));
			dataStore.putColumn(DISTANCE_KEY, ArrayUtils.toPrimitive(distances.toArray(new Integer[vendorIds.size()])));
			dataStore.putColumn(PICK_UP_DATE_KEY, ArrayUtils.toPrimitive(pickUpDate.toArray(new Long[vendorIds.size()])));
			dataStore.putColumn(DROP_OFF_DATE_KEY, ArrayUtils.toPrimitive(dropOffDates.toArray(new Long[vendorIds.size()])));
			dataStore.putColumn(PASSENGER_KEY, ArrayUtils.toPrimitive(passengers.toArray(new Integer[vendorIds.size()])));
			dataStore.putColumn(RATING_KEY, ArrayUtils.toPrimitive(ratings.toArray(new Short[vendorIds.size()])));
			dataStore.putColumn(STORED_FWD_KEY, ArrayUtils.toPrimitive(storeFwdFlags.toArray(new Character[vendorIds.size()])));
			dataStore.putColumn(PICK_UP_LOCATION_KEY, ArrayUtils.toPrimitive(pickUpLocations.toArray(new Integer[vendorIds.size()])));
			dataStore.putColumn(DROP_OFF_LOCATION_KEY, ArrayUtils.toPrimitive(dropOffLocations.toArray(new Integer[vendorIds.size()])));
			dataStore.putColumn(PAYMENT_KEY, ArrayUtils.toPrimitive(paymentTypes.toArray(new Integer[vendorIds.size()])));
			dataStore.putColumn(FARE_KEY, ArrayUtils.toPrimitive(fares.toArray(new Integer[vendorIds.size()])));
			dataStore.putColumn(EXTRA_KEY, ArrayUtils.toPrimitive(extras.toArray(new Integer[vendorIds.size()])));
			dataStore.putColumn(MTA_TAX_KEY, ArrayUtils.toPrimitive(mtaTaxes.toArray(new Integer[vendorIds.size()])));
			dataStore.putColumn(TIP_KEY, ArrayUtils.toPrimitive(tips.toArray(new Integer[vendorIds.size()])));
			dataStore.putColumn(TOLLS_KEY, ArrayUtils.toPrimitive(tolls.toArray(new Integer[vendorIds.size()])));
			dataStore.putColumn(IMPRoVEMENT_SURCHARGE_KEY, ArrayUtils.toPrimitive(improvementSurcharges.toArray(new Integer[vendorIds.size()])));
			dataStore.putColumn(TOTAL_AMOUNT_KEY, ArrayUtils.toPrimitive(totalAmounts.toArray(new Integer[vendorIds.size()])));
			
			
			return vendorIds.size();
		} catch (FileNotFoundException e) {
			throw new BusinessException("File [" + file.getName() + "] not found: " + e.getMessage(), e);
		} catch (IOException e) {
			throw new BusinessException("Could not read file [" + file.getName() + "]: " + e.getMessage(), e);
		}
	}
	
	
	private void setColumns() {
		vendorId = this.dataAccessor.getDataStore().getIntegerColumns().get(VENDOR_ID_KEY);
		pickUpDate = this.dataAccessor.getDataStore().getLongColumns().get(PICK_UP_DATE_KEY);
		dropOffDate = this.dataAccessor.getDataStore().getLongColumns().get(DROP_OFF_DATE_KEY);
		passengerCount = this.dataAccessor.getDataStore().getIntegerColumns().get(PASSENGER_KEY);
		distance = this.dataAccessor.getDataStore().getIntegerColumns().get(DISTANCE_KEY);
		rating = this.dataAccessor.getDataStore().getShortColumns().get(RATING_KEY);
		storeFwdFlag = this.dataAccessor.getDataStore().getCharColumns().get(STORED_FWD_KEY);
		pickUpLocation = this.dataAccessor.getDataStore().getIntegerColumns().get(PICK_UP_LOCATION_KEY);
		dropOffLocation = this.dataAccessor.getDataStore().getIntegerColumns().get(DROP_OFF_LOCATION_KEY);
		paymentType = this.dataAccessor.getDataStore().getIntegerColumns().get(PAYMENT_KEY);
		fare = this.dataAccessor.getDataStore().getIntegerColumns().get(FARE_KEY);
		extra = this.dataAccessor.getDataStore().getIntegerColumns().get(EXTRA_KEY);
		mtaTax = this.dataAccessor.getDataStore().getIntegerColumns().get(MTA_TAX_KEY);
		tip = this.dataAccessor.getDataStore().getIntegerColumns().get(TIP_KEY);
		tolls = this.dataAccessor.getDataStore().getIntegerColumns().get(TOLLS_KEY);
		improvementSurcharge = this.dataAccessor.getDataStore().getIntegerColumns().get(IMPRoVEMENT_SURCHARGE_KEY);
		totalAmount = this.dataAccessor.getDataStore().getIntegerColumns().get(TOTAL_AMOUNT_KEY);
	}
	
	
	public Integer getVendorId() {
		return vendorId[partialCursor];
	}
	
	
	public Long getPickUpDate() {
		return pickUpDate[partialCursor];
	}
	
	
	public Long getDropOffDate() {
		return dropOffDate[partialCursor];
	}
	
	
	public Integer getPassengerCount() {
		return passengerCount[partialCursor];
	}
	
	
	public Integer getDistance() {
		return distance[partialCursor];
	}
	
	
	public Short getRating() {
		return rating[partialCursor];
	}
	
	
	public Character getStoreFwdFlag() {
		return storeFwdFlag[partialCursor];
	}
	
	
	public Integer getPickUpLocation() {
		return pickUpLocation[partialCursor];
	}
	
	
	public Integer getDropOffLocation() {
		return dropOffLocation[partialCursor];
	}
	
	
	public Integer getPaymentType() {
		return paymentType[partialCursor];
	}
	
	
	public Integer getFare() {
		return fare[partialCursor];
	}
	
	
	public Integer getExtra() {
		return extra[partialCursor];
	}
	
	
	public Integer getMtaTax() {
		return mtaTax[partialCursor];
	}
	
	
	public Integer getTip() {
		return tip[partialCursor];
	}
	
	
	public Integer getTolls() {
		return tolls[partialCursor];
	}
	
	
	public Integer getImprovementSurcharge() {
		return improvementSurcharge[partialCursor];
	}
	
	
	public Integer getTotalAmount() {
		return totalAmount[partialCursor];
	}
	
	
	public boolean hasNext() {
		return partialCursor < partialLength - 1;
	}
	
	
	public boolean next() {
		partialCursor++;
		return hasNext();
	}
	
	
	@Override
	public void reset() {
		partialCursor = offset;
	}
	
	
	public Stream<TaxiRide> stream() {
		return StreamSupport.stream(new TaxiRide.TaxiRideSpliterator(new TaxiRide(this.dataAccessor, 0, this.dataAccessor.getLength()), this.dataAccessor), true);
	}
	
	
	public static class TaxiRideSpliterator implements Spliterator<TaxiRide> {
		
		
		private final GenericDataAccessor dataAccessor;
		private final int MIN_WORKLOAD_THRESHOLD = 5000;
		@Getter
		private final TaxiRide taxiRide;
		private int cursorOffset = 0;
		
		
		public TaxiRideSpliterator(GenericDataAccessor dataAccessor) {
			this.dataAccessor = dataAccessor;
			this.taxiRide = null;
		}
		
	
	public long estimateSize() {
		return dataAccessor.getLength();
			this.dataAccessor = dataAccessor;
			this.taxiRide = taxiRide;
			this.cursorOffset = taxiRide.getOffset();
		}
		
		
		@Override
		public boolean tryAdvance(Consumer<? super TaxiRide> action) {
			if (taxiRide.hasNext()) {
				taxiRide.next();
				action.accept(this.getTaxiRide());
				return true;
			} else {
				return false;
			}
		}
	
		
		@Override
		public Spliterator<TaxiRide> trySplit() {
			if (!dataAccessor.hasNext()) {
				return null;
			}
			
			int skipped = Math.max(0, dataAccessor.getCursor());
			int rowsToProcess = dataAccessor.getLength() - skipped;
			int remainingRows = rowsToProcess - cursorOffset;
			if (remainingRows <= 0) {
				return null;
			} else if (remainingRows < MIN_WORKLOAD_THRESHOLD) {
				TaxiRide taxiRide = new TaxiRide(dataAccessor, cursorOffset, dataAccessor.getLength());
				cursorOffset = dataAccessor.getLength();
				return new TaxiRideSpliterator(taxiRide, dataAccessor);
			}
			
			int cores = Runtime.getRuntime().availableProcessors();
			int batchSize = Math.floorDiv(rowsToProcess, cores);
			int rest = rowsToProcess % cores;
			int partialLength = batchSize + rest;
			
			TaxiRide taxiRide = new TaxiRide(dataAccessor, cursorOffset, partialLength);
			
			cursorOffset += partialLength;
			
			return new TaxiRideSpliterator(taxiRide, dataAccessor);
	}
		
		
	public int getLength() {
		return dataAccessor.getLength();
	}
		
		
		@Override
		public int characteristics() {
			return Spliterator.NONNULL | Spliterator.IMMUTABLE | Spliterator.SIZED;
		}
	}
}
