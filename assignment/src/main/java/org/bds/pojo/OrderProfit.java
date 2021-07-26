package org.bds.pojo;

import java.io.Serializable;

public class OrderProfit implements Comparable<OrderProfit>{  

	/**
	 * 
	 */
	
	String orderId;

	public String getOrderId() {
		return orderId;
	}

	public void setOrderId(String orderId) {
		this.orderId = orderId;
	}

	public Double getProfit() {
		return profit;
	}

	public void setProfit(Double profit) {
		this.profit = profit;
	}

	Double profit;

	
	 public int compareTo(OrderProfit o) {
	        return this.getProfit() .compareTo(o.getProfit());
	    }
}
