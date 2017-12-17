package cs555.FileSystem.node;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import cs555.FileSystem.transport.CommandThread;
import cs555.FileSystem.transport.TCPSender;
import cs555.FileSystem.transport.TCPServerThread;
import cs555.FileSystem.util.DetectionOfFailure;
import cs555.FileSystem.wireformats.ErrorCorrFromChunkServerRequest;
import cs555.FileSystem.wireformats.ErrorCorrFromChunkServerResponse;
import cs555.FileSystem.wireformats.Event;
import cs555.FileSystem.wireformats.EventFactory;
import cs555.FileSystem.wireformats.FailureDetectionRequest;
import cs555.FileSystem.wireformats.NewReplicationMessage;
import cs555.FileSystem.wireformats.Protocol;
import cs555.FileSystem.wireformats.ReadControllerResponse;
import cs555.FileSystem.wireformats.ReadFailedControllerResponse;
import cs555.FileSystem.wireformats.RequestMajorHeartbeat;
import cs555.FileSystem.wireformats.StoreFileControllerRequest;
import cs555.FileSystem.wireformats.StoreFileControllerResponse;


public class Controller implements Node{

	private int portNumber = -1;
	private EventFactory eventFactory;
	private static TCPServerThread server = null;
	public static ArrayList<String> chunkServers = new ArrayList<String>();
	private static HashMap<String, String> fileChunkServerInfo = new HashMap<String, String>();
	private static HashMap<String, String> fileMetaDataInfo = new HashMap<String, String>();
	private static HashMap<String, Integer> fileChunkCount = new HashMap<String, Integer>();
	public static HashMap<String, String> NodeFileList = new HashMap<String, String>();
	public static HashMap<String, Long> failureTimeStamp = new HashMap<String, Long>();
	public static HashMap<String, Integer> heartBeatCount = new HashMap<String, Integer>();
	Random randomNode = new Random();
	private CommandThread commandInput;
	private DetectionOfFailure detectFailure;
	
	// Initializing the Controller Node
	public void Initialize(String[] args)
	{	
		if(args.length != 1)
		{
			System.out.println("Enter Port Number");
			return;
		}
			
		portNumber = Integer.parseInt(args[0]);
			
		// Initializing Event Factory Singleton Instance
		eventFactory = EventFactory.getInstance();
		eventFactory.setNodeInstance(this);
		
		// Initializing the server thread
		server = new TCPServerThread(portNumber,  Controller.class.getSimpleName(), eventFactory);
		Thread serverThread = new Thread(server);
		serverThread.start();
		
		commandInput = new CommandThread(1);
		Thread commandThread = new Thread(commandInput);
		commandThread.start();
		
		detectFailure = new DetectionOfFailure(this);
		Thread failureDetectionThread = new Thread(detectFailure);
		failureDetectionThread.start();
	}
	
	private void sendChunkServerInformation(String clientIP, int clientPort)
	{
		Socket msgSocket = null;
		
		int index1 = randomNode.nextInt(chunkServers.size());
		int index2 = randomNode.nextInt(chunkServers.size());
		while(index2 == index1)
		{
			index2 = randomNode.nextInt(chunkServers.size());
		}
		int index3 = randomNode.nextInt(chunkServers.size());
		while((index3 == index2))
		{
			index3 = randomNode.nextInt(chunkServers.size());
		}
		while((index3 == index1))
		{
			index3 = randomNode.nextInt(chunkServers.size());
		}
		
		String info = chunkServers.get(index1) + "#" + chunkServers.get(index2) + "#" + chunkServers.get(index3);
		
		// sending response message
		try {
			msgSocket = new Socket(clientIP, clientPort);
		} catch (IOException e) {
			e.printStackTrace();
		};
		
		//sending lookup message
		try {
			TCPSender sender =  new TCPSender(msgSocket);
			StoreFileControllerResponse stFlContRes= new StoreFileControllerResponse();
			byte[] dataToSend = stFlContRes.storeFileControllerResponse(info);
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
	}
	
	private void sendChunkServerInfoToClient(String clientIP, int clientPort, String fileName, int numSplits)
	{
		Socket msgSocket = null;
		
		try {
			msgSocket = new Socket(clientIP, clientPort);
		} catch (IOException e) {
			e.printStackTrace();
		};
		
		//int numOfChunks = fileChunkCount.get(fileName);
		
		for(int i = 1; i <= numSplits; i++)
		{
			String chunkFileName = fileName + ".00" + i;
			String info = chunkFileName + "#" + fileChunkServerInfo.get(chunkFileName).split(",")[0];

			try {
				TCPSender sender =  new TCPSender(msgSocket);
				ReadControllerResponse readContRes= new ReadControllerResponse();
				byte[] dataToSend = readContRes.readFileControllerResponse(info);
				sender.sendData(dataToSend);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		try {
			msgSocket.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void readFailedChunkServerInfoToClient(String clientIP, int clientPort, String information)
	{
		String reqFile = information.split("#")[0];
		String prevFailedNodeInfo = information.split("#")[1].trim();
		String info = "";
		int i = 0;

//		while(i < 3)
//		{
//			if(!fileChunkServerInfo.get(reqFile).split(",")[i].equals(prevFailedNodeInfo))
//			{
//				info = reqFile + "#" + fileChunkServerInfo.get(reqFile).split(",")[i];
//				break;
//			}
//			i++;
//		}
		System.out.println("File: " + reqFile);
		info = reqFile + "#" + fileChunkServerInfo.get(reqFile).split(",")[1];
		System.out.println("Inside readFailedChunkServerInfoToClient: " + info);
		
		Socket msgSocket = null;
		
		try {
			msgSocket = new Socket(clientIP, clientPort);
		} catch (IOException e) {
			e.printStackTrace();
		};
		
		try {
			TCPSender sender =  new TCPSender(msgSocket);
			ReadFailedControllerResponse readFailedContRes= new ReadFailedControllerResponse();
			byte[] dataToSend = readFailedContRes.readFailedControllerResponse(info);
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
		
		
	}
	
	private void sendOtherReplicaInfoForErrCorr(String chunkServerIP, int chunkServerPort, String fileName)
	{
		boolean flag = false;
		if(fileChunkServerInfo.containsKey(fileName))
		{
			String[] temp = fileChunkServerInfo.get(fileName).split(",");
			for(String s: temp)
			{
				if(!s.equals(chunkServerIP + ":" + chunkServerPort))
				{
					if(!flag){
					Socket msgSocket = null;

					try {
						msgSocket = new Socket(chunkServerIP, chunkServerPort);
					} catch (IOException e) {
						e.printStackTrace();
					};
					try {
						TCPSender sender =  new TCPSender(msgSocket);
						ErrorCorrFromChunkServerResponse errCorrFrmChnkServRes= new ErrorCorrFromChunkServerResponse();
						byte[] dataToSend = errCorrFrmChnkServRes.errorCorrFromChunkServerResponse(fileName + "#" + s);
						sender.sendData(dataToSend);
						flag = true;
					} catch (IOException e) {
						e.printStackTrace();
					}		
					try {
						msgSocket.close();
						msgSocket = null;
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}}
				}
			}
		}
	}
	
	@Override
	public void onEvent(Event e) {
		
		int messageType = e.getMessageType();
		
		if(messageType == Protocol.STORE_FILE_CONTROLLER_REQUEST)
		{
			//Sending 3 chunk server information to client
			fileChunkCount.put(e.getInfo(), e.getForwardFlag());
			sendChunkServerInformation(e.getIPAddress(), e.getListenPortNumber());
		}
		else if(messageType == Protocol.MINOR_HEARTBEAT_MESSAGE)
		{
			synchronized(this)
			{
				String IPPortCombination = e.getIPAddress() + ":" + e.getListenPortNumber();
				System.out.println("Info: " + e.getInfo());
				
				if(heartBeatCount.containsKey(IPPortCombination))
				{
					int value = heartBeatCount.get(IPPortCombination);
					heartBeatCount.put(IPPortCombination, value+1);
				}
				else
				{
					heartBeatCount.put(IPPortCombination, 1);
					// Request a major heart beat
					Socket msgSocket = null;

					try {
						msgSocket = new Socket(InetAddress.getByName(e.getIPAddress()), e.getListenPortNumber());
					} catch (IOException e1) {
						e1.printStackTrace();
					};
					try {
						TCPSender sender =  new TCPSender(msgSocket);
						RequestMajorHeartbeat reqMajorHrtBeat= new RequestMajorHeartbeat();
						byte[] dataToSend = reqMajorHrtBeat.requestMajorHeartbeat();
						sender.sendData(dataToSend);
					} catch (IOException e2) {
						e2.printStackTrace();
					}		
					try {
						msgSocket.close();
						msgSocket = null;
					} catch (IOException e3) {
						// TODO Auto-generated catch block
						e3.printStackTrace();
					}
				}
				
				if(!chunkServers.contains(IPPortCombination))
				{
					chunkServers.add(IPPortCombination);
				}
				if((!e.getInfo().trim().equals("NONE")) && (!e.getInfo().trim().equals("")))
				{	
					String[] token = e.getInfo().split(",");
					for(int i = 0; i < token.length; i++)
					{
						System.out.println("token0= " + token[i].split("#")[0] + " token1= " + token[i].split("#")[1] + " IPPort= " + IPPortCombination);
						String info1 = "";
						String info2 = "";
						if(fileChunkServerInfo.containsKey(token[i].split("#")[0]))
						{
							String temp = fileChunkServerInfo.get(token[i].split("#")[0]);
							if(!temp.contains(IPPortCombination))
							{
								info1 = fileChunkServerInfo.get(token[i].split("#")[0]) + "," + IPPortCombination;
								fileChunkServerInfo.put(token[i].split("#")[0], info1);
							}
						}
						else
						{
							fileChunkServerInfo.put(token[i].split("#")[0], IPPortCombination);
						}
						
						if(fileMetaDataInfo.containsKey(token[i].split("#")[0]))
						{
							String temp = fileMetaDataInfo.get(token[i].split("#")[0]);
							if(!temp.contains(token[i].split("#")[1]))
							{
								info2 = fileMetaDataInfo.get(token[i].split("#")[0]) + "," + token[i].split("#")[1];
								fileMetaDataInfo.put(token[i].split("#")[0], info2);
							}
						}
						else
						{
							fileMetaDataInfo.put(token[i].split("#")[0], token[i].split("#")[1]);
						}
					}
				}
			}
		}
		else if(messageType == Protocol.MAJOR_HEARTBEAT_MESSAGE)
		{
			synchronized(this)
			{
				String IPPortCombination = e.getIPAddress() + ":" + e.getListenPortNumber();
				System.out.println(e.getInfo());
				if(!chunkServers.contains(IPPortCombination))
				{
					chunkServers.add(IPPortCombination);
				}
			
				if((!e.getInfo().trim().equals("NONE")) && (!e.getInfo().trim().equals("")))
				{	
					String[] token = e.getInfo().split(",");
					for(int i = 0; i < token.length; i++)
					{
						System.out.println("token0= " + token[i].split("#")[0] + " token1= " + token[i].split("#")[1] + " IPPort= " + IPPortCombination);
						String info1 = "";
						String info2 = "";
						if(fileChunkServerInfo.containsKey(token[i].split("#")[0]))
						{
							String temp = fileChunkServerInfo.get(token[i].split("#")[0]);
							if(!temp.contains(IPPortCombination))
							{
								info1 = fileChunkServerInfo.get(token[i].split("#")[0]) + "," + IPPortCombination;
								fileChunkServerInfo.put(token[i].split("#")[0], info1);
							}
						}
						else
						{
							fileChunkServerInfo.put(token[i].split("#")[0], IPPortCombination);
						}
						
						if(fileMetaDataInfo.containsKey(token[i].split("#")[0]))
						{
							String temp = fileMetaDataInfo.get(token[i].split("#")[0]);
							if(!temp.contains(token[i].split("#")[1]))
							{
								info2 = fileMetaDataInfo.get(token[i].split("#")[0]) + "," + token[i].split("#")[1];
								fileMetaDataInfo.put(token[i].split("#")[0], info2);
							}
						}
						else
						{
							fileMetaDataInfo.put(token[i].split("#")[0], token[i].split("#")[1]);
						}
					}
				}
			}
		}
		else if(messageType == Protocol.READ_FILE_CONTROLLER_REQUEST)
		{
			sendChunkServerInfoToClient(e.getIPAddress(), e.getListenPortNumber(), e.getInfo(), e.getForwardFlag());
		}
		else if(messageType == Protocol.ERROR_CORRECTION_FROM_CHUNKSERVER_REQUEST)
		{
			sendOtherReplicaInfoForErrCorr(e.getIPAddress(), e.getListenPortNumber(), e.getInfo());
		}
		else if(messageType == Protocol.READ_FAILED_CONTROLLER_REQUEST)
		{
			readFailedChunkServerInfoToClient(e.getIPAddress(), e.getListenPortNumber(), e.getInfo());
		}
		else if(messageType == Protocol.FAILURE_DETECTION_RESPONSE)
		{
			Date dte=new Date();
		    long timestamp = dte.getTime();
			failureTimeStamp.put(e.getIPAddress() + ":" + e.getListenPortNumber(), timestamp);
			NodeFileList.put(e.getIPAddress() + ":" + e.getListenPortNumber(), e.getInfo());
//			for(Map.Entry<String, String> entry : NodeFileList.entrySet())
//			{
//				System.out.println(entry.getKey() + "-> " + entry.getValue());
//			}
		}
	}
	
	public void printChunkServers()
	{
		for(int i = 0; i < chunkServers.size(); i++)
		{
			System.out.println("ChunkServer " + i + "-> " + chunkServers.get(i));
		}
	}
	
	public void printFileChunkServerInfo()
	{
		for(Map.Entry<String, String> entry : fileChunkServerInfo.entrySet())
		{
			System.out.println(entry.getKey() + "-> " + entry.getValue());
		}
	}
	
	public void printFileMetaDataInfo()
	{
		for(Map.Entry<String, String> entry : fileMetaDataInfo.entrySet())
		{
			System.out.println(entry.getKey() + "-> " + entry.getValue());
		}
	}
	
	public void replicateToANewNodeOnFailure(String IPPortInfo)
	{
		String[] failedNodeFileList = NodeFileList.get(IPPortInfo).split(",");
		for(int i = 0; i < failedNodeFileList.length; i++)
		{
			String newReplicaNode = "";
			String[] replNodes = fileChunkServerInfo.get(failedNodeFileList[i]).split(",");
			for(int j = 0; j < chunkServers.size(); j++)
			{
				int k = 0;
				int count = 0;
				while(k < replNodes.length)
				{
					if(!chunkServers.get(j).equals(replNodes[k]))
					{
						count++;
					}
					k++;
				}
				if(count == 3)
				{
					newReplicaNode = chunkServers.get(j);
					break;
				}
			}
			System.out.println("New replica Node: " + i + "->" + newReplicaNode);
			// Send a message to this new node to store the replica
			int l = 0;
			String fromReplicationInfo = "";
			while(l < replNodes.length)
			{
				if(!IPPortInfo.equals(replNodes[l]))
				{
					fromReplicationInfo = replNodes[l];
					break;
				}
				l++;
			}
			
			Socket msgSocket = null;
			
			try {
				msgSocket = new Socket(InetAddress.getByName(newReplicaNode.split(":")[0]), Integer.parseInt(newReplicaNode.split(":")[1]));
			}
			catch (IOException e) {
				e.printStackTrace();
			};
			
			try {
				TCPSender sender =  new TCPSender(msgSocket);
				NewReplicationMessage newReplMsg= new NewReplicationMessage();
				byte[] dataToSend = newReplMsg.newReplicationMessage(failedNodeFileList[i] + "#" + fromReplicationInfo);
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
		// Updating the data structures
		chunkServers.remove(IPPortInfo);
		String[] failedNodeFileList1 = NodeFileList.get(IPPortInfo).split(",");
		for(int i = 0; i < failedNodeFileList1.length; i++)
		{
			String newList = "";
			String[] replNodes = fileChunkServerInfo.get(failedNodeFileList[i]).split(",");
			int j = 0;
			while(j < replNodes.length)
			{
				if(!replNodes[j].equals(IPPortInfo))
				{
					newList += replNodes[j];
					if(j+1 != replNodes.length)
						newList += ",";
				}
				j++;
			}
			System.out.println("NewList: " + newList);
			fileChunkServerInfo.put(failedNodeFileList[i], newList);
		}
		NodeFileList.remove(IPPortInfo);
	}
	
	public static void main(String[] args) {
		
		Controller controller =  new Controller();
		// Initialize the Discovery node
		controller.Initialize(args);
	}


}
