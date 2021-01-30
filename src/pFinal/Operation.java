package pFinal;

import java.io.Serializable;
//import java.util.Set;
import java.util.HashMap;


public class Operation implements Serializable {

	private static final long serialVersionUID = 1L;
	private OperationEnum operation;
	private DHT_Map map = null;
	
	
	public Operation (OperationEnum operation,
			DHT_Map map)           {
		this.operation = operation;
		this.map       = map;
	}
	
	public OperationEnum getOperation() {
		return operation;
	}

	public void setOperation(OperationEnum operation) {
		this.operation = operation;
	}

	public String getKey() {
		return this.map.getKey();
	}

	public void setKey(String key) {
		this.map.setKey(key);
	}
	public Integer getValue() {
		return this.map.getValue();
	}

	public void setValue(Integer value) {
		this.map.setValue(value);;
	}

	public DHT_Map getMap() {
		return map;
	}

	public void setMap(DHT_Map map) {
		this.map = map;
	}
	
}