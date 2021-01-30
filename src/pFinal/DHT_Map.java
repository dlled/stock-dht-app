package pFinal;

import java.io.Serializable;

public class DHT_Map implements Serializable {
	
	private String key;
	private Integer value;
	
	public DHT_Map(String key, Integer value ) {
		this.key = key;
		this.value = value;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public Integer getValue() {
		return value;
	}

	public void setValue(Integer value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return "DHT_Map [key=" + key + ", value=" + value + "]";
	}
	
}
 