package cs555.FileSystem.util;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

import cs555.FileSystem.node.ChunkServer;
import cs555.FileSystem.transport.TCPSender;
import cs555.FileSystem.wireformats.MajorHeartbeatMessage;
import cs555.FileSystem.wireformats.MinorHeartbeatMessage;

public class MinorHeartbeat implements Runnable{
	
	private int PERIOD = 30; //seconds
	private String DEFAULT_NAME = "MinorHeartbeat";
	private HashMap<String, String> values; // <id, value>
	private volatile boolean done = false;
	private InetAddress m_controllerNodeHost = null;
	private int m_controllerNodePort = -1;
	private ChunkServer m_chunkServer = null;
	private String ownIP = "";
	private int ownPort = -1;
	private String mapout = "";
	private int count = 0;


	public MinorHeartbeat(ChunkServer chunkServer, InetAddress controllerNodeHost, int controllerNodePort, String IP){
		m_chunkServer = chunkServer;
		m_controllerNodeHost = controllerNodeHost;
		m_controllerNodePort = controllerNodePort;
		ownIP = IP;
		values = new HashMap<String, String>();
	}

	public void setDone()
	{
		done = true;
	}

	private void minorCollect() {
		
	    // Get the newly added nodes
		HashMap<String, String> diff = new HashMap<String, String>();		
		if(!values.isEmpty())
		{
			diff.putAll(m_chunkServer.chunkInformation);
			for (String s: values.keySet()) {
				if (diff.containsKey(s))
					diff.remove(s);
				else
					diff.put(s, values.get(s));
			}
		
			//mapout = "{";
			for(Map.Entry<String, String> entry : diff.entrySet())
			{
				mapout += entry.getKey() + "#" + entry.getValue() + ",";
			}
			//mapout += " }";
		}
		else if(!m_chunkServer.chunkInformation.isEmpty())
		{	
			if(values.isEmpty())
			{
				values.putAll(m_chunkServer.chunkInformation);
		
				//mapout = "{";
				for(Map.Entry<String, String> entry : values.entrySet())
				{
					mapout += entry.getKey() + "#" + entry.getValue() + ",";
				}
				//mapout += " }";
			}
		}
		else
		{
			mapout = "NONE";
		}
	}
	
	private void majorCollect() {
	    
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

	public void sendData(String type){
	    
		// Sending the Metadata information to controller
		Socket msgSocket = null;
				
		// Sending message to Chunk Server to store chunks
		try {
			msgSocket = new Socket(m_controllerNodeHost, m_controllerNodePort);
		}
		catch (ConnectException e) {
		    // TODO Auto-generated catch block
		    System.out.println("Can't connect to the Controller");
		    return;
		}
		catch (IOException e) {
			e.printStackTrace();
		};
				
		//sending StoreFileControllerRequest message
		try {
			TCPSender sender =  new TCPSender(msgSocket);
			ownPort = m_chunkServer.server.getOwnPort();
			if(type == "MINOR")
			{
				MinorHeartbeatMessage minHrtbtMsg= new MinorHeartbeatMessage();
				byte[] dataToSend = minHrtbtMsg.minorHeartbeatMessage(ownIP, ownPort, mapout);
				sender.sendData(dataToSend);
			}
			else if(type == "MAJOR")
			{
				MajorHeartbeatMessage majHrtbtMsg= new MajorHeartbeatMessage();
				byte[] dataToSend = majHrtbtMsg.majorHeartbeatMessage(ownIP, ownPort, mapout);
				sender.sendData(dataToSend);
			}
			
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
	        count++;
	        if(count == 10)
	        {
	        	this.majorCollect();
		        this.sendData("MAJOR");
		        count = 0;
	        }
	        else
	        {
	        	this.minorCollect();
	        	this.sendData("MINOR");
	        }
	        // Let the thread sleep for a while.
	        Thread.sleep(PERIOD * 1000);
	     }
	 } catch (InterruptedException e) {
	     System.out.println("Thread " +  DEFAULT_NAME + " interrupted.");
	 }
	 System.out.println("Thread " +  DEFAULT_NAME + " exiting.");
	}
}
