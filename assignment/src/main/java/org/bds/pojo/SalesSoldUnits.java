package org.bds.pojo;

public class SalesSoldUnits implements Comparable<SalesSoldUnits> {

	public String getYear() {
		return year;
	}

	public void setYear(String year) {
		this.year = year;
	}

	public Double getUnitsSold() {
		return unitsSold;
	}

	public void setUnitsSold(Double unitsSold) {
		this.unitsSold = unitsSold;
	}

	String year;
	Double unitsSold;

	public int compareTo(SalesSoldUnits o) {
		return this.getUnitsSold().compareTo(o.getUnitsSold());
	}
}
