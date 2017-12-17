package cs555.FileSystem.util;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;

import cs555.FileSystem.node.Controller;
import cs555.FileSystem.transport.TCPSender;
import cs555.FileSystem.wireformats.FailureDetectionRequest;


public class DetectionOfFailure implements Runnable{
	
	private int PERIOD = 10; //seconds
	private String DEFAULT_NAME = "FailureDetection";
	private volatile boolean done = false;
	private Controller m_controller = null;

	public DetectionOfFailure(Controller controller){
		m_controller = controller;
	}

	public void setDone()
	{
		done = true;
	}

	public void sendData(String IP, int port){
		
		// Sending the Metadata information to controller
		Socket msgSocket = null;
		
		// Sending message to Chunk Server to store chunks
		try {
			msgSocket = new Socket(InetAddress.getByName(IP), port);
		}
		catch (ConnectException e) {
		    // TODO Auto-generated catch block
		    System.out.println("Can't send to " + IP + " and " + port);
		    return;
		}
		catch (IOException e) {
			e.printStackTrace();
		};
		
		//sending StoreFileControllerRequest message
		try {
			TCPSender sender =  new TCPSender(msgSocket);
			FailureDetectionRequest failDetReq= new FailureDetectionRequest();
			String pingData = "Are you alive?";
			byte[] dataToSend = failDetReq.failureDetectionRequest(pingData);
			sender.sendData(dataToSend);
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		
		try {
			msgSocket.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void run() {
	  System.out.println("Running " +  DEFAULT_NAME );
	  try {
	     while(!done) {
	        if(m_controller.chunkServers.size() > 0)
	        {
	        	for(int i = 0; i < m_controller.chunkServers.size(); i++)
	        	{
	        		System.out.println("Sending to: " + m_controller.chunkServers.get(i));
	        		this.sendData(m_controller.chunkServers.get(i).split(":")[0], Integer.parseInt(m_controller.chunkServers.get(i).split(":")[1]));
	        	}
	        }
	        // Let the thread sleep for a while.
	        Thread.sleep(PERIOD * 1000);
	        // Checking for node failure
	        Date dte=new Date();
		    long timestamp = dte.getTime();
		    String IPPortInfo = "";
		    for(Map.Entry<String, Long> entry : m_controller.failureTimeStamp.entrySet())
		    {
		    	long timeInSeconds = entry.getValue() / 1000;
		    	long timeDiff = (timestamp/1000) - timeInSeconds;
		    	System.out.println("Time Difference: " + timeDiff);
		    	if(timeDiff > 30)
		    	{
		    		System.out.println("Node " + entry.getKey() + " has been failed!");
		    		m_controller.replicateToANewNodeOnFailure(entry.getKey());
		    		IPPortInfo = entry.getKey();
		    	}
		    }
    		//synchronized (m_controller.failureTimeStamp) {
		    if(!IPPortInfo.equals(""))
		    {
    			m_controller.failureTimeStamp.remove(IPPortInfo);
		    }
    		//}
		    
	     }
	 } catch (InterruptedException e) {
	     System.out.println("Thread " +  DEFAULT_NAME + " interrupted.");
	 }
	 System.out.println("Thread " +  DEFAULT_NAME + " exiting.");
	}
}
