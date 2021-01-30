package pFinal;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.ConsoleHandler;

import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;


import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class Server extends Thread implements Serializable, Watcher, StatCallback{
	
	private static final int TIMEOUT = 5000;
	private static final int DELAY = 300;
	
	// PATHS DE LOS NODES DE ZOOKEEPER
	private static String nodeServers = "/prueba2Server";
	private static String nodeSingleServer = "Server-";
	
	// Valores de los nodos de operaciones y nodo de datos recibidos del ensemble
	private String nodeOperations= "/OPTS1";
	private String receivedDataPath = "/rx7";
	private List<String> serverlist;
	
	// Almacenadores de operaciones, creamos un set para utilizar le contains
	// y así podemos obtener de una forma rápida las operaciones nuevas
	private List<String> opList;
	private Set<String> opSet;
	
	public String host = "127.0.0.1:2181";
	// Guía de distribución de las réplicas 
		/*								
		 * TO => T1    => Server 0 (T0, T2)
		 * T1 => T2	   => Server 1 (T1, T0)	
		 * T2 => T0	   => Server 2 (T2, T1)	
		 * 
		 * */
	
	Integer[] dist_rep = {1, 2, 0};
	
	//Tabla principal del server
	public HashMap<Integer, Integer> DHT = new HashMap<Integer, Integer>(); 
	
	// Tabla réplica del server
	public HashMap<Integer, Integer> DHTRep = new HashMap<Integer, Integer>();
	
	// Identificador server en zookeeper
	public String serverId; 
	
	// Identificador del server dentro del ensemble funcional
	private Integer ensembleId; 
	
	// Instancia del objeto de zk
	public static ZooKeeper zk; 
	
	// Objeto para crear, borrar nodos y manejo de datos en los nodos
	private BufferZk buffer; 
	
	private static void print(Object obj) {
		System.out.println((String) obj);
	}
	
	
	public void process(WatchedEvent event) {
		try {
			System.out.println("Unexpected invocated this method. Process of the object");
		} catch (Exception e) {
			System.out.println("Unexpected exception. Process of the object");
		}
	}
	@Override
	public void processResult(int arg0, String arg1, Object arg2, Stat arg3) {
		// TODO Auto-generated method stub
		
	}
	public void setDHT(HashMap<Integer, Integer> map) {
		this.DHT.putAll(map);
	}
	public void setDHTRep(HashMap<Integer, Integer> map) {
		this.DHTRep.putAll(map);
	}

	public HashMap<Integer, Integer> getDHT(){
		return (HashMap<Integer, Integer>) this.DHT.clone();
	}
	public HashMap<Integer, Integer> getDHTRep(){
		return (HashMap<Integer, Integer>) this.DHTRep.clone();
	}
	
	public void destroyServer() {
		try {
			currentThread().interrupt();
		} catch (Exception e) {
			print("Server no destruido" + e.toString());
		}
	}


	public Server() {
		
		//CONFIGURACIÓN INICIAL DE LA SESSION CON ZOOKEEPER
		try {
			if (zk == null) {
				zk = new ZooKeeper(host, TIMEOUT, sessionWatcher);
				
			}
		} catch(Exception e) {
			System.out.println("Error at creation the zookeeper session, try it later");
		}
		
		//CONFIGURACIÓN INICIAL DEL NODO PADRE DE SERVIDORES
		try {
			if (zk != null) {
				Stat s = zk.exists(nodeServers, false);
				if (s == null) {
					//CREAMOS EL NODO PADRE DE LA RAMA DE SERVIDORES
					String resp = zk.create(nodeServers, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				}
			}
		} catch (Exception e) {
		print("Error at creating the principal node server");
		}
				
		
		//CONFIGURACIÓN INICIAL DEL NODO DE OPERACIONES Y SUBSCRIPCIÓN AL EVENTO
		try {
			Stat opStat = zk.exists(nodeOperations, false);
			if (opStat == null) {
				String resp = zk.create( nodeOperations, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
		
		//ASIGNACION DE LOS DIFERENTES WATCHERS A LOS PATHS DE SERVERS Y OPERACIONES
			this.serverlist = zk.getChildren(nodeServers, false, opStat);	
			this.opList =  zk.getChildren(nodeOperations, opWatcher);
			this.opSet = Set.copyOf(this.opList);
		//CREAMOS EL NODO QUE ALOJARÁ EL SERVER CUANDO LO CREEMOS
			String resp = zk.create(nodeServers + "/" + nodeSingleServer, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
			this.serverId = resp.replace(nodeServers + "/", "");
		} catch (Exception e) {
			print("Error al crear el nodod de operaciones del ensemble de zookeeper: " + e.toString());
		}
		
		//CONFIGURACIÓN INICIAL DEL BUFFER DEL SERVIDOR
		try {
			//print("Creando buffer para el servidor" + getServerId());
			this.buffer = new BufferZk();
		} catch(Exception e) {
			print("Error al crear el buffer en " + getServerId());
		}

		
	}
	// WATCHER DE INICIO DE SESIÓN DEL SERVER
	private  Watcher sessionWatcher = new Watcher() {
		public void process(WatchedEvent event) {
			System.out.println("Zookeeper session has been established");
			System.out.println(event.toString());
			notify();
		}
	};
	
	//WATCHER DE OPERACIONES
	private  Watcher opWatcher = new Watcher() {
		public synchronized void process(WatchedEvent event) {
			if(ensembleId == -1) {
				return;
			}
			String nodePath = event.getPath();
			// 
			//print("OPERACION AVISTADA EN "+ nodePath +" EN SERVIDOR " + getServerId() + " ensemble " + getEnsembleId());
			try {
				wait(DELAY);
				List<String> operationList = zk.getChildren(nodePath, opWatcher);
				
				//DEPENDIENDO DE LA OPERACIÓN, EL SERVIDOR TOMARÁ UNAS ACCIONES U OTRAS
				for(String operation : operationList) {
					if(! opSet.contains(operation)) {
						opSet = Set.copyOf(operationList); //Comprobación de si operación nueva
						String key;
						Integer value;
						Integer destServer;
						Integer repServer;
						//print("Operación no avistada anteriormente " + operation);
						Operation op = (Operation) buffer.receiveData(operation);
						//print("Recibido en servidor " + getServerId() + " el objeto " + op.getMap().toString() + "operacion de " + op.getOperation().toString());
						switch(op.getOperation()) {
						case PUT_MAP:
							key = op.getKey();
							value = op.getValue();
							destServer = chooseServer(key);
							repServer = dist_rep[destServer];
							if(destServer == getEnsembleId()) {
								Integer keyHashed = key.hashCode();
								DHT.put(keyHashed, value);
								print("valor colocado en la tabla " + getEnsembleId());
							} 
							if(repServer == getEnsembleId()) {
								Integer keyHashed = key.hashCode();
								DHTRep.put(keyHashed, value);
								print("valor replicado en la tabla " + getEnsembleId());
							}
							break;
						case GET_MAP:
							key = op.getKey();
							destServer = chooseServer(key);
							repServer = dist_rep[destServer];
							if(destServer == getEnsembleId()) {
								Integer keyHashed = key.hashCode();
								Integer res = DHT.get(keyHashed);
								DHT_Map toSendOr = new DHT_Map(key, res);
								if(toSendOr.getValue() == null) {
									print("valor no encontrado en la tabla principal");
								} else {
									print("valor encontrado en tabla principal, enviando...");
									sendOperation(buffer, OperationEnum.RETURN_VALUE, toSendOr);
								}
							} 
							if(repServer == getEnsembleId()) {
								Integer keyHashed = key.hashCode();
								Integer res = DHTRep.get(keyHashed);
								DHT_Map toSendRep = new DHT_Map(key, res);
								if(toSendRep.getValue() == null) {
									print("valor no encontrado en la tabla replica");
								} else {
									print("valor encontrado en tabla replica, enviando...");
									sendOperation(buffer, OperationEnum.RETURN_VALUE_REP, toSendRep);
								}
							}
							break;
						case REMOVE_MAP:
							key = op.getKey();
							destServer = chooseServer(key);
							repServer = dist_rep[destServer];
							if(destServer == getEnsembleId()) {
								Integer keyHashed = key.hashCode();
								DHT.remove(keyHashed);
								print("valor borrado en la tabla " + getEnsembleId());
							} 
							if(repServer == getEnsembleId()) {
								Integer keyHashed = key.hashCode();
								DHTRep.remove(keyHashed);
								print("valor borrado en la tabla replica" + getEnsembleId());
							}
							break;
						case CONTAINS_KEY_MAP:
							key = op.getKey();
							destServer = chooseServer(key);
							repServer = dist_rep[destServer];
							if(destServer == getEnsembleId()) {
								Integer keyHashed = key.hashCode();
								Integer res = DHT.get(keyHashed);
								if(res == null) {
									print("clave no encontrado en la tabla principal");
								} else {
									print("clave encontrado en la tabla " + getEnsembleId());;
								}
							} 
							if(repServer == getEnsembleId()) {
								Integer keyHashed = key.hashCode();
								Integer res = DHTRep.get(keyHashed);
								if(res == null) {
									print("clave no encontrado en la tabla replica");
								} else {
									print("clave encontrado en la tabla replica " + getEnsembleId());;
								}
							}
							break;
						
						default:
							//print("No se identifica la operación realizada");
							break;
						}
						
						//print("Servidor actualizado");
					}
				}				
			} catch (Exception e) {
				print("Error while getting childs of node operation in server" + e.toString());
			}
			notifyAll();
		}
	};
	
	public Integer getEnsembleId() {
		return this.ensembleId;
	}
	public String getServerId() {
		return this.serverId;
	}
	public void setEnsembleId(Integer ensembleId) {
		this.ensembleId = ensembleId;
	}
	
	// Para elegir el server, realizamos una segmentación del espacio total que puede dar lugar
	// el método hashcode() de la clave, mediante el valor que nos dé, lo guardamos en uno de los 
	// tres servidores del ensemble.
	
	// Siguiendo la teoría de P2P y las hashtables, nuestra implementación del algoritmo de elección
	private static int chooseServer(String key) {
		//OBSERVAMOS EL TAMAÑO DEL SISTEMA, EN ESTE CASO, LO GUARDAMOS COMO SI FUESE ILIMITADO, AUNQUE SEAN 3
		int clusterSize = 3;
		Integer max_value = Integer.MAX_VALUE;	
		Integer min_value = 0;
		double hash = Math.abs(key.hashCode());
		for(int i=1 ; i <= clusterSize ; i++) {
			//DEPENDIENDO DEL HASH CALCULADO, LO MANDAMOS A UN SERVIDOR O A OTRO
			double factor = (double) i / (double) clusterSize;
			int umbral = (int) (factor * max_value);
			if (  hash <= umbral) {
				return i-1;
			}
		}
		return -1;
	}
	private synchronized void sendOperation(BufferZk buffer, OperationEnum opType, DHT_Map map) {
		String path = "/OPTS1";
		String nodePath = "/op-";
		
		Operation operation = new Operation(opType, map);
		
		
		
		buffer.setPath(path, nodePath);
		
		String childId = buffer.createNodeChild();
		
		print("Node de operacion creado");
		
		buffer.sendData(operation, childId);
		
		print("Datos mandados");
		
		
		
		
	}
	
}
