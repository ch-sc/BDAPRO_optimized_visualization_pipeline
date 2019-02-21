package de.tuberlin.dima.bdapro.data;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import lombok.Builder;
import lombok.Getter;

@Getter
public class DataStore {
	
	private Map<UUID, int[]> integerColumns = new HashMap<>();
	private Map<UUID, double[]> doubleColumns = new HashMap<>();
	private Map<UUID, CharSequence[]> textColumns = new HashMap<>();
	private Map<UUID, long[]> longColumns = new HashMap<>();
	private Map<UUID, short[]> shortColumns = new HashMap<>();
	private Map<UUID, char[]> charColumns = new HashMap<>();
	
	
	public UUID putColumn(UUID uuid, int[] column) {
		integerColumns.put(uuid, column);
		return uuid;
	}
	
	
	public UUID putColumn(UUID uuid,long[] column) {
		longColumns.put(uuid, column);
		return uuid;
	}
	
	
	public UUID putColumn(UUID uuid,double[] column) {
		doubleColumns.put(uuid, column);
		return uuid;
	}
	
	
	public UUID putColumn(UUID uuid,CharSequence[] column) {
		textColumns.put(uuid, column);
		return uuid;
	}
	
	
	public UUID putColumn(UUID uuid,short[] column) {
		shortColumns.put(uuid, column);
		return uuid;
	}
	
	
	public UUID putColumn(UUID uuid,char[] column) {
		charColumns.put(uuid, column);
		return uuid;
	}
}
