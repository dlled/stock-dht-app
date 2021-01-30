package pFinal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class BufferZk {
	
	public static ZooKeeper zk;
	
	private static String path = "";
	private static String nodePath = "";
	public String childId;
	
	public synchronized void setPath(String path, String nodePath) {
		this.path = path;
		this.nodePath = nodePath;
	}
	public synchronized String[] getPath() {
		String[] paths = {this.path, this.nodePath};
		return paths;
	}
	
	private static final int TIMEOUT = 5000;
	private  Watcher sessionWatcher = new Watcher() {
		public void process(WatchedEvent event) {
			System.out.println("Zookeeper session has been established");
			System.out.println(event.toString());
			notify();
		}
	};
	
	/* BUFFERZK
	 * 
	 *  This class will permit to operate with Zookeeper Ensemble of any
	 *  node we want to create.
	 * 
	 *  To set a new path, use setPath, and don´t forget to cache the previous
	 *  path, this will let us to don´t lose our app context, and use 
	 *  the buffer in any part of our code. To cache your path, you can use getPath.
	 *  
	 *  To create a node, you can use createNodeChild (PERSISTENT-SEQ NODE) or createEphemeralChild 
	 *  to create an EPHEMERAL NODE.
	 *  
	 *  To remove a node, set first the path, then, use nodeChildId to remove the specific node.
	 *  
	 *  To send data, use sendData with a Serializable Object, to receiveData, use receiveData
	 *  this will return an object of type Object, you could use a casting to parse an Object 
	 *  to your preferred Serializable Object Type.
	 *  
	 */
	
	public BufferZk() {
		String host = "127.0.0.1:2181";
	
		try {
			if( zk == null) {
				zk = new ZooKeeper(host, TIMEOUT, sessionWatcher);
				
			}
		}  catch (Exception e) {
			System.out.println("Exception in constructor");
		}
		System.out.println("Buffer conectado a zookeeper");
	}
	
	// Crear un nodo hijo en el path que nos dan
	public synchronized String createPersistentChild(String child) {
		String pathChild = "";
		String response = "";
		if (zk != null) {
			try {
				//Comprobamos si existe el nodo padre
				
				Stat s = zk.exists(path, false);
				//Si no, lo creamos
				if (s == null) {
					// Created the znode, if it is not created.
					response = zk.create(path, new byte[0], 
							Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				}
				//Creamos al nodo hijo
				Stat s2 = zk.exists(this.path  +  child, false);
				if(s2 == null) {
					String childResp = zk.create(this.path  +  child, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					pathChild = childResp.replace(this.path + '/', "");
				} else {
					pathChild = child.replace("/", "");
				}
				
				
				//Devolvemos su referencia de nodo hijo
				childId = pathChild;
				
			} catch (Exception e) {
				System.out.println("error in buffer zk at creating child node" + e.toString());
			}
		}
		return pathChild;
	}
	// Igual que el anterior, solo que estos nodos son permanentes
	public synchronized String createNodeChild() {
		String pathChild = "";
		String response = "";
		if (zk != null) {
			try {
				
				Stat s = zk.exists(path, false);
				//Comprobamos si existe el nodo padre, si no, lo creamos
				if (s == null) {
					// Created the znode, if it is not created.
					response = zk.create(path, new byte[0], 
							Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				}
				
				String childResp = zk.create(this.path  +  this.nodePath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
				
				pathChild = childResp.replace(this.path + '/', "");
				childId = pathChild;
				
			} catch (Exception e) {
				System.out.println("error in buffer zk at creating child node" + e.toString());
			}
		}
		return pathChild;
	}
	
	public synchronized void sendData(Object obj, String childID) {
		
		byte[] data = null;
		int v = -1;
		/* 
		 * http://chuwiki.chuidiang.org/index.php?title=Serialización_de_objetos_en_java
		 * 
		 * ByteArrayOutputStream bs= new ByteArrayOutputStream();
		 * ObjectOutputStream os = new ObjectOutputStream (bs);
		 * [...]] y más que se ilustra abajo
		 * 
		 * Para poder mandar operaciones a los servidores, vamos a 
		 * tener que serializar los diferentes objetos que queranos meter como identificables
		 */
		try {
			ByteArrayOutputStream bs= new ByteArrayOutputStream();
			ObjectOutputStream os = new ObjectOutputStream (bs);
			os.writeObject(obj); // this es de tipo DatoUdp
			os.flush();
			
			data = bs.toByteArray();
		} catch (Exception e) {
			System.out.println("error sending data in byte parsing:    " + e.toString());
		}
		try {
			zk.setData(this.path + "/" + childID, data, v);
		} catch (Exception e) {
			System.out.println("error sending data in zookeeper data send: " + e.toString());
		}
	}
	
	
	public synchronized Object receiveData(String childID) {
		byte[] data = null;
		Object object = null;
		
		try {
			Stat stat = zk.exists(this.path + "/" + childID, false);
			if (stat != null) {
				data = zk.getData(this.path + "/" + childID, false, stat);
			} else {
				System.out.println("path nulo");
			}
		} catch (Exception e) {
			System.out.println("Error receiving data from zookeeper, error: " + e.toString());
		}
		
		if (data != null) {
			
			try {
				
				ByteArrayInputStream bs= new ByteArrayInputStream(data);
				ObjectInputStream is = new ObjectInputStream (bs);
				
				try {
					object = (Object) is.readObject();
				} catch (Exception e) {
					System.out.println("Error in casting bytes object " + e.toString());
				}
				
				
			} catch (Exception e) {
				System.out.println("error parsind data bytes to casting object" + e.toString());
			}
		}
		return object;

	}
	public synchronized boolean removeNode(String childId) {
		try {
			//Comprobamos si existe el nodo a borrar
			Stat stat = zk.exists(this.path + "/" + childId, false);
			
			if(stat != null) {
				//System.out.println("Removiendo nodo " + childId);
				zk.delete(this.path + "/" + childId, stat.getVersion());
				System.out.println("Nodo " + childId + " eliminado");
				return true;
			} else {
				System.out.println("El nodo que intentas borrar no existe");
			}
		} catch(Exception e) {
			System.out.println("Error removing node " + e.toString());
		}
		return false;
	}
	
}
