package cs555.FileSystem.util;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

import cs555.FileSystem.node.ChunkServer;
import cs555.FileSystem.transport.TCPSender;
import cs555.FileSystem.wireformats.MajorHeartbeatMessage;

public class MajorHeartbeat implements Runnable{
	
	private int PERIOD = 300; //seconds
	private String DEFAULT_NAME = "MajorHeartbeat";
	private HashMap<Integer, Object> values; // <id, value>
	private volatile boolean done = false;
	private InetAddress m_controllerNodeHost = null;
	private int m_controllerNodePort = -1;
	private ChunkServer m_chunkServer = null;
	private String ownIP = "";
	private int ownPort = -1;
	private static String mapout = "";


	public MajorHeartbeat(ChunkServer chunkServer, InetAddress controllerNodeHost, int controllerNodePort, String IP){
		m_chunkServer = chunkServer;
		m_controllerNodeHost = controllerNodeHost;
		m_controllerNodePort = controllerNodePort;
		ownIP = IP;
		values = new HashMap<Integer,Object>();
	}

	public void setDone()
	{
		done = true;
	}

	public void collect() {
	    
		// Collecting all the chunk meta data information which is to be sent to controller
		if(!m_chunkServer.chunkInformation.isEmpty()){
			//mapout = "{";
			for(Map.Entry<String, String> entry : m_chunkServer.chunkInformation.entrySet())
			{
				mapout += entry.getKey() + "#" + entry.getValue() + ",";
			}
			//mapout += " }";
		}
		else
		{
			mapout = "NONE";
		}
	}

	public void sendData(){
		
		// Sending the Metadata information to controller
		Socket msgSocket = null;
		try {
			msgSocket = new Socket(m_controllerNodeHost, m_controllerNodePort);
		} catch (IOException e) {
			e.printStackTrace();
		};

		try {
			TCPSender sender =  new TCPSender(msgSocket);
			ownPort = m_chunkServer.server.getOwnPort();
			MajorHeartbeatMessage majHrtbtMsg= new MajorHeartbeatMessage();
			byte[] dataToSend = majHrtbtMsg.majorHeartbeatMessage(ownIP, ownPort, mapout);
			sender.sendData(dataToSend);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		try {
			msgSocket.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		mapout = "";
	}

	public void run() {
	  System.out.println("Running " +  DEFAULT_NAME );
	  try {
	     while(!done) {
	        System.out.println("Thread: " + DEFAULT_NAME + ", " + "I'm alive");

	        //this.collect();
	        //this.sendData();
	        // Let the thread sleep for a while.
	        Thread.sleep(PERIOD * 1000);
	     }
	 } catch (InterruptedException e) {
	     System.out.println("Thread " +  DEFAULT_NAME + " interrupted.");
	 }
	 System.out.println("Thread " +  DEFAULT_NAME + " exiting.");
	}
}
