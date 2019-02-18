package de.tuberlin.dima.bdapro.data;

import java.util.stream.Stream;

import de.tuberlin.dima.bdapro.data.taxi.TaxiRide;

public interface Streamable <T> {
	Stream<T> stream();
}
