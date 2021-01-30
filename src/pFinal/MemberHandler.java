package pFinal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;



public class MemberHandler {
	
	private Boolean[] freeIndex;
	private String pathServers = "/prueba2Server";
	private String host = "127.0.0.1:2181";
	private static final int SESSION_TIMEOUT = 5000;
	private static final int DELAYRX = 2500;
	private static final int DELAY = 2000;

	private BufferZk buffer;
	private static  ZooKeeper zk;

	private Server[] workingServers = new Server[3];
	
	private boolean automaticSync = false;
	
	private Set<String> oldServSet;
	private List<String> servList;
	
	
	private String nodeOperations= "/OPTS1";
	
	private List<String> opList;
	private Set<String> opSet;

	
	// Creamos un set con las peticiones que hemos mandado y no han sido respondidas por los servidores.
	
	private static List<Server> serverList = new ArrayList<Server>();
	
	private Watcher cWatcher = new Watcher() {
		public void process (WatchedEvent e) { 
			System.out.println("Created session");
			System.out.println(e.toString());
			notify();
		}
	};
	
	// To get the number of working servers in the ensemble [0,1,2]
	private Integer getNumberWorkingServers() {
		Integer n_servers = 0;
		for(Server s: workingServers) {
			if(s!=null) {
				n_servers++;
			}
		}
		return n_servers;
	}
	
	
	//WATCHER DE OPERACIONES
		private  Watcher opWatcher = new Watcher() {
			public synchronized void process(WatchedEvent event) {
				String nodePath = event.getPath();

				try {
					wait(DELAYRX);
					//print("OPERACION AVISTADA EN "+ nodePath +" EN APP ");
					List<String> operationList = zk.getChildren(nodePath, opWatcher);
					//print(operationList.toString());
					//DEPENDIENDO DE LA OPERACIÓN, EL SERVIDOR TOMARÁ UNAS ACCIONES U OTRAS
					for(String operation : operationList) {
						if(! opSet.contains(operation)) {
							//print("Operación no avistada anteriormente " + operation);
							Operation op = (Operation) buffer.receiveData(operation);
							//print("Recibido el objeto " + op.getMap().toString() + "operacion de " + op.getOperation().toString());
							switch(op.getOperation()) {
							case PUT_MAP:
								break;
							case GET_MAP:
								break;
							case REMOVE_MAP:
								break;
							case CONTAINS_KEY_MAP:
								break;
							case RETURN_VALUE:
								print("VALOR RECIBIDO DE LA TABLA PRINCIPAL: " + op.getValue().toString());
								break;
							case RETURN_VALUE_REP:
								print("VALOR RECIBIDO DE LA TABLA REPLICA: " + op.getValue().toString());
								break;
								
							default:
								print(op.getOperation());
								//print("No se identifica la operación realizada" + op.getMap().toString());
								break;
							
							}
						}
					}
					opSet = Set.copyOf(operationList);
					
				} catch (Exception e) {
					print("Error while getting childs of node operation " + e.toString());
				}
				notifyAll();
			}
		};
	
	// Watcher used to get the session when changes are produced
	private Watcher watcherMember = new Watcher() {
		public synchronized void process(WatchedEvent event) {
			try {
				wait(1000);
				
				print("Evento observado en " + pathServers);
				servList = zk.getChildren(pathServers,  watcherMember);
				
				if(servList.size() < oldServSet.size()) {
					print("Se ha eliminado un server");
					oldServSet = Set.copyOf(servList);
					if(automaticSync) {
						rebuildServers();
					} else {
						print("Ensemble desincronizado, activar sincronización o hacerlo de forma manual");
					}
					
				} else {
					print("Se ha añadido un server");
				}
				print("Lista global de servidores: " + servList.toString());
				oldServSet = Set.copyOf(servList);
				print("Nº de servidores en el ensemble: " + getNumberWorkingServers());
			} catch(Exception e) {
				print("Exception in WatcherMember in MemberHandler" + e.toString());
			}
		}
	}; 
	
	// Function used to send operation to the ensemble
	private synchronized void sendOperation(BufferZk buffer, OperationEnum opType, DHT_Map map) {
		String path = "/OPTS1";
		String nodePath = "/op-";
		
		Operation operation = new Operation(opType, map);
		
		buffer.setPath(path, nodePath);
		
		String childId = buffer.createNodeChild();
		
		//print("Node de operacion creado");
		
		buffer.sendData(operation, childId);
		
		//print("Datos mandados");
		
		
	}
	
	public synchronized void changeSyncMode() {
		if(automaticSync) {
			automaticSync = false;
			print("sincronización automática desactivada");
		}else {
			automaticSync = true;
			print("sincronizzación automática activada");
		}
	}
	
	// Function to get key and value for the user in
	public DHT_Map putMap(Scanner sc) {
		String  key     = null;
		Integer value   = 0;

		print("Enter product (String) = ");
		key = sc.next();


		System. out .print("Enter quantity (Integer) = ");
		if (sc.hasNextInt()) {
			value = sc.nextInt();
		} else {
			System.out.println("enter an integer");
			sc.next();
			return null;
		}

		return new DHT_Map(key, value);
	}
	
	//Function to get key from the user interface
	public DHT_Map putKey(Scanner sc) {
		String  key     = null;
		Integer value   = 0;

		System. out .print("Enter product (String) = ");
		key = sc.next();
		value = 0;

		return new DHT_Map(key, value);
	}
	
	
	
	public MemberHandler() {
		this.freeIndex = new Boolean[3];
		this.buffer = new BufferZk();
		for(int i=0; i<this.freeIndex.length; i++) {
			this.freeIndex[i] = true;
		}
		
		try {
			if(zk == null) {
				zk = new ZooKeeper(host, SESSION_TIMEOUT, cWatcher);
			}
			try {
				// Wait for creating the session. Use the object lock
				wait();
				Server.zk = zk;
				BufferZk.zk = zk;
				//zk.exists("/",false);
			} catch (Exception e) {
				// TODO: handle exception
			}
		} catch(Exception e) {
			System.out.println("Error initializing ZK" + e.toString());
		}
		
		if (zk != null) {
			try {
				String response = new String();
				Stat s = zk.exists(pathServers, watcherMember); 
				if (s == null) {
					// Created the znode, if it is not created.
					response = zk.create(pathServers, new byte[0], 
							Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					//System.out.println(response);
				}
				try {
					Stat opStat = zk.exists(nodeOperations, false);
					if (opStat == null) {
						String resp = zk.create( nodeOperations, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					}
				} catch(Exception e) {
					print(e.toString());
				}
				//Conseguimos los servidores ya creados
				servList = zk.getChildren(pathServers, watcherMember, s); //this, s)
				
				// Hacemos la copia de los servers que ya tenemos
				oldServSet = Set.copyOf(servList);
				
				//Colocamos un watcher en la lista de datos recibidos, para poder enterarnos cuando nos los manden
				//receivedData = zk.getChildren(receivedDataPath, serverDataWatcher);	
				
				this.opList =  zk.getChildren(nodeOperations, opWatcher);
				this.opSet = Set.copyOf(this.opList);
				
				
			} catch (KeeperException e) {
				System.out.println(e);
				System.out.println("The session with Zookeeper failes. Closing");
				return;
			} catch (InterruptedException e) {
				System.out.println("InterruptedException raised");
			}

		}
		
		print("Member Handler inicializado");
	}
	private Integer giveMeFreeIndex() {
		for(int i=0; i < freeIndex.length; i++) {
			if(freeIndex[i]) {
				return i;
			}
		}
		return -1;
	}
	private Integer findFallenServer() {
		for(int i=0; i < this.workingServers.length; i++) {
			if(this.workingServers[i] == null) {
				return (Integer) i;
			} 
		}
		return -1;
	}
	private synchronized void buildEnsemble(Integer i) {
		try {
			boolean out = false;
			Server server = new Server();
			serverList.add(server);
			print("Server " + server.getServerId() + " añadido a la lista global de servers");
			if(i==-1) {
				out = true;
			} else {
				freeIndex[i]=false;
				wait(1000);
				server.setEnsembleId(i);
				workingServers[i] = server;
				print("Server añadido al ensemble con indexEnsemble " + i);
			}
			
			if(out) {
				server.setEnsembleId(-1);
				print("Server " + server.getServerId() + " añadido a la lista global de servers" );
			}
			print("Servidor procesado");
			wait(2000);
		} catch(Exception e) {
			print("Error al construir el ensemble" + e.toString());
		}
		
		
	}
	public synchronized void createServer() {
		Integer index = this.giveMeFreeIndex();
		this.buildEnsemble(index);
	}
	
	public synchronized void printWorkingServers() {
		if(this.getNumberWorkingServers() == 0) {
			print("En este momento no hay servidores funcionando");
		} else {
			print("--------------------------------------------------");
			print("---------- SERVERS EN FUNCIONAMIENTO -------------");
			print("--------------------------------------------------");
			for(Server s: this.workingServers) {
				if (s != null) {
					print("-- SERVER: " + s.getServerId() + " con ensembleId  " + s.getEnsembleId()+ "  --");
					print("--------------------------------------------------");
				}
			}
			print("--------------------------------------------------");
		}
			
	}
	public synchronized void printServerDHT() {
		if(this.getNumberWorkingServers() == 0) {
			print("En este momento no hay servidores funcionando");
		} else {
			print("--------------------------------------------------");
			print("--------- DISTRIBUTED HASHTABLES ONLINE ----------");
			print("--------------------------------------------------");
			for(Server s: this.workingServers) {
				if (s != null) {
					print("-- SERVER: " + s.getServerId() + " con ensembleId  " + s.getEnsembleId()+ "  --");
					print("--------------------------------------------------");
					print("-------------------- DHT -------------------------");
					print("--------------------------------------------------");
					s.getDHT().forEach((k,v) -> print("\n-----     KEY: " + k + " VALUE: " + v  + "\n"));
					print("--------------------------------------------------");
					print("------------------- DHTREP -----------------------");
					print("--------------------------------------------------");
					s.getDHTRep().forEach((k,v) -> print("\n-----     KEY: " + k + " VALUE: " + v + "\n"));
					print("--------------------------------------------------");
				}
			}
			print("--------------------------------------------------");
			
		}
			
	}
	public synchronized void printAllServers() {
		
			print("--------------------------------------------------");
			print("----------------- TOTAL SERVERS ------------------");
			print("--------------------------------------------------");
			for(Server s: this.serverList) {
				if (s != null) {
					print("-- SERVER: " + s.getServerId() + " con ensembleId  " + s.getEnsembleId()+ "  --");
					print("--------------------------------------------------");
				}
			print("--------------------------------------------------");
			
		}
			
	}

	public synchronized void rebuildServers() {
		Integer index = this.findFallenServer();
		print("hay que recuperar el index " + index);
		Integer myDHTRep = (index + 1) % 3;
		print("COGEREMOS LA TABLA Original DEL SERVIDOR " + myDHTRep);
		Integer myDHT = (index + 2) % 3;
		print("COGEREMOS LA TABLA Replica DEL SERVIDOR " + myDHT);
		if(index != -1) {
			this.buildEnsemble(index);
			try {
				wait(DELAY);
			}catch(Exception e){
				print("error en delay " + e.toString());
			}
			this.workingServers[index].setDHTRep((HashMap<Integer, Integer>) this.workingServers[myDHT].getDHT().clone());
			this.workingServers[index].setDHT((HashMap<Integer, Integer>) this.workingServers[myDHTRep].getDHTRep().clone());
			print("LAS TABLAS RESTAURADAS SERÍAN: ");
			print(this.workingServers[index].getDHT().toString());
			print("DATOS RESTAURADOS DE SERVER " + index );
			
		}
	}

	public synchronized void removeServer(Integer index) {
		if(workingServers[index] != null) {
			String path = "/prueba2Server";
			String nodePath = "/Server-";
			print("SE VA A BORRAR EL SERVER DEL ENSEMBLE " + workingServers[index].getEnsembleId());
		
			buffer.setPath(path, nodePath);
			buffer.removeNode(workingServers[index].getServerId());
			Server toRemove = workingServers[index];
			toRemove.setEnsembleId(-1);
			workingServers[index] = null;
			serverList.remove(toRemove);
			
			freeIndex[index] = true;
			print("Servidor con id de ensemble " + index +" borrado");
		} else {
			print("No existe un servidor con ese index ");
		}
		
	}
	
	
	// EL PRINT DE PYTHON
	private static void print(Object obj) {
		System.out.println(obj);
	}
	
	

	public static void main(String[] args) throws InterruptedException {

		boolean correct = false;
		int     menuKey = 0;
		int		appKey = 0;
		boolean inApp = false;
		boolean inServerAdmin = false;
		boolean exit    = false;
		Scanner sc      = new Scanner(System.in);
		
		MemberHandler mh = new MemberHandler();
		
		TimeUnit.SECONDS.sleep(1);
		
		while (!exit) {
			try {
				correct = false;
				menuKey = 0;
				while (!correct) {
					print("WELCOME TO THE FCON APP");
					print(" 1) Server Admin Menu \n 2) App Menu \n 0) Exit. \n");		
					if (sc.hasNextInt()) {
						menuKey = sc.nextInt();
						correct = true;
					} else {
						sc.next();
						System.out.println("The provised text provided is not an integer");
					}
					
				}

				switch (menuKey) {
				case 1:
					inServerAdmin = true;
					
					while (inServerAdmin) {
						print("SERVER ADMINISTRATION MENU");
						print("Select an option: ");
						System.out.println(" 1) Create server \n 2) Print working servers. \n 3) Print all servers. \n 4) Remove server \n 5) Activate automatic sync \n 6) Sync fallen server \n 0) Exit. \n");				
						if (sc.hasNextInt()) {
							appKey = sc.nextInt();							
						} else {
							sc.next();
							System.out.println("The provised text provided is not an integer");
						}
						switch(appKey) {
						case 1: 
							print("Create server");
							mh.createServer();
							break;
						case 2: 
							print("Print working servers");
							mh.printWorkingServers();
							
							break;
						case 3:
							print("Print all servers");
							mh.printAllServers();
							break;
							
						case 4: 
							print("Introduce el server a borrar: ");
							if(sc.hasNextInt()) {
								Integer removeIndex = sc.nextInt();
								print("Borrando server con índice" + removeIndex);
								mh.removeServer(removeIndex);
								print("server borrado de zookeeper");
							}
							break;
						case 5:
							print("Change sync mode");
							mh.changeSyncMode();
							break;
						case 6:
							print("Syncing servers manually");
							mh.rebuildServers();
							break;
						
						case 0: 
							print("ADIOS!");
							inServerAdmin = false;
							break;
						}
						
					}
					break;
					
				case 2:
					inApp = true;
					
					while (inApp) {
						print("WELCOME TO STOCK APP MENU");
						print("Select an option: ");
						System.out.println(" 1) Show data. \n 2) Put data. \n 3) Get data. \n 4) Remove data  \n 0) Exit. \n");				
						if (sc.hasNextInt()) {
							appKey = sc.nextInt();							
						} else {
							sc.next();
							System.out.println("The provised text provided is not an integer");
						}
						switch(appKey) {
						case 1: 
							print("SHOW ALL data");
							mh.printServerDHT();
							break;
						case 2: 
							print("Put data");
							print("Send new obj operation");
							
							DHT_Map map = mh.putMap(sc);
							
							print("Se va a enviar el obj: " + map.toString());
							
							mh.sendOperation(mh.buffer, OperationEnum.PUT_MAP, map);
							
							print("Operacion enviada");
							break;
						case 3:
							print("Get data");
							
							DHT_Map mapG = mh.putKey(sc);
							
							print("Se va a pedir el obj: " + mapG.toString());							
							
							mh.sendOperation(mh.buffer, OperationEnum.GET_MAP, mapG);
							
							print("Operacion enviada");
							break;
							
						case 4: 
							print("Remove data");
							
							DHT_Map mapR = mh.putKey(sc);
							
							print("Se va a borrar el obj: " + mapR.toString());
							
							mh.sendOperation(mh.buffer, OperationEnum.REMOVE_MAP, mapR);
							
							print("Operacion enviada");
							break;
						
						case 0: 
							print("ADIOS!");
							inApp = false;
							break;
						}
						
					}
					break;
				
				case 0:
					exit = true;	
				default:
					break;
				
				}
				
			} catch (Exception e) {
				System.out.println("Exception at Main. Error read data");
				System.err.println(e);
				e.printStackTrace();
			}
	
		}
	
		sc.close();
		

	}

}
