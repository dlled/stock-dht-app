package pFinal;

import java.util.ArrayList;
import java.util.Set;

public interface DHT_UI {
	
	DHT_Map put(DHT_Map map);
	
	DHT_Map get(DHT_Map map);
	
	DHT_Map remove(DHT_Map map);
	
	boolean contains(String mapKey);
	
	Set<String> keySet();
	
	ArrayList<Integer> valueSet();

}
