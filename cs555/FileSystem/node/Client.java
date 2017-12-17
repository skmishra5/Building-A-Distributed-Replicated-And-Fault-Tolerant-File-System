package cs555.FileSystem.node;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import cs555.FileSystem.transport.CommandThread;
import cs555.FileSystem.transport.TCPSender;
import cs555.FileSystem.transport.TCPServerThread;
import cs555.FileSystem.wireformats.Event;
import cs555.FileSystem.wireformats.EventFactory;
import cs555.FileSystem.wireformats.Protocol;
import cs555.FileSystem.wireformats.ReadChunkServerRequest;
import cs555.FileSystem.wireformats.ReadControllerRequest;
import cs555.FileSystem.wireformats.ReadFailedControllerRequest;
import cs555.FileSystem.wireformats.StoreFileChunkServerRequest;
import cs555.FileSystem.wireformats.StoreFileControllerRequest;

public class Client implements Node{

	private static String ownIP = "";
	private static int ownServerPort = -1;
	private static InetAddress controllerNodeHost = null;
	private static int controllerNodePort = -1;
	private EventFactory eventFactory;
	private static TCPServerThread server;
	private static Thread serverThread;
	private CommandThread commandInput;
	private static int m_numOfChunkPartitions = 1;
	private static int numPartitions = 0;
	private static String m_fileName = "";
	private static String readfileName = "";
	private static int numReadFilePartitions = 0;
	private static int numOfFilesToBeMerged = 0;
	private boolean failureFlag = false;
	private String readFailedNodeInfo = "";
	
	
	// Initializing the peer node
	public void Initialize(String[] args)
	{
		// Checking number of command line arguments which is one
		if(args.length != 2)
		{
			System.out.println("Enter Controller IP, Controller Port Number.");
			return;
		}
		
		System.out.println("Initializing the Client");
		
		// Getting the Controller IP and port number from the command line
		try {
			controllerNodeHost = InetAddress.getByName(args[0]);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} 	
		
		controllerNodePort = Integer.parseInt(args[1]);
		
		try {
			ownIP = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		
		// Initializing Event Factory Singleton Instance
		eventFactory = EventFactory.getInstance();
		eventFactory.setNodeInstance(this);
		
		// Initializing the server thread to listen to other connections
		server = new TCPServerThread(0,  Client.class.getSimpleName(), eventFactory);
		serverThread = new Thread(server);
		serverThread.start();
		
		commandInput = new CommandThread(0);
		Thread commandThread = new Thread(commandInput);
		commandThread.start();
	}
	
	public void storeFile(String fileName)
	{
		m_fileName = fileName;		
		// Split the file into 64KB chunks
		try {
			numPartitions = splitFile(m_fileName);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// Using the flow of Controller and Chunk Servers to store chunk files
		handleFileStorage();
	}
	
	private int splitFile(String fileName) throws FileNotFoundException, IOException
	{
		int partCounter = 1;

		int sizeOfFiles = 1024 * 64;// 64KB
		byte[] buffer = new byte[sizeOfFiles];
		
		//String[] token = fileName.split("/");

		//try-with-resources to ensure closing stream
		try (FileInputStream fis = new FileInputStream(fileName);
		BufferedInputStream bis = new BufferedInputStream(fis)) 
		{
			int bytesAmount = 0;
			while ((bytesAmount = bis.read(buffer)) > 0) {
				//write each chunk of data into separate file with different number in name
				String filePartName = String.format("%s.%03d", fileName, partCounter++);
				//File newFile = new File("Chunks", filePartName);
				try (FileOutputStream out = new FileOutputStream("./Chunks/" + filePartName)) {
					out.write(buffer, 0, bytesAmount);
				}
			}
		}
		
		return partCounter - 1;
	}
	
	private void handleFileStorage()
	{
		Socket msgSocket = null;
		
		// Sending message to Controller asking for replication nodes
		try {
			msgSocket = new Socket(controllerNodeHost, controllerNodePort);
		} catch (IOException e) {
			e.printStackTrace();
		};
		
		//sending StoreFileControllerRequest message
		try {
			TCPSender sender =  new TCPSender(msgSocket);
			StoreFileControllerRequest stFlContReq= new StoreFileControllerRequest();
			ownServerPort = server.getOwnPort();
			byte[] dataToSend = stFlContReq.storeFileControllerRequest(ownIP, ownServerPort, m_fileName, numPartitions);
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
	
	private void sendChunksToChunkServer(String info)
	{
		Socket msgSocket = null;
				
		// Sending message to Chunk Server to store chunks
		try {
			msgSocket = new Socket(InetAddress.getByName(info.split("#")[0].split(":")[0]), Integer.parseInt(info.split("#")[0].split(":")[1]));
		} catch (IOException e) {
			e.printStackTrace();
		};
		
		//sending StoreFileControllerRequest message
		try {
			TCPSender sender =  new TCPSender(msgSocket);
			StoreFileChunkServerRequest stFlchunkReq= new StoreFileChunkServerRequest();
			byte[] dataToSend = stFlchunkReq.fileStoreRequest("./Chunks/" + m_fileName + ".00" + m_numOfChunkPartitions, m_fileName + ".00" + m_numOfChunkPartitions, info, ownIP, ownServerPort);
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
		
		m_numOfChunkPartitions += 1;
	}
	
	public void readFile(String fileName, int numSplits)
	{
		Socket msgSocket = null;
		readfileName = fileName;
		numReadFilePartitions = 0;
		failureFlag = false;
		
		// Sending message to Controller asking for replication nodes
		try {
			msgSocket = new Socket(controllerNodeHost, controllerNodePort);
		} catch (IOException e) {
			e.printStackTrace();
		};
		
		//sending StoreFileControllerRequest message
		try {
			TCPSender sender =  new TCPSender(msgSocket);
			ReadControllerRequest readContReq= new ReadControllerRequest();
			ownServerPort = server.getOwnPort();
			byte[] dataToSend = readContReq.readFileControllerRequest(ownIP, ownServerPort, m_fileName, numSplits);
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
	
	private void sendReadRequestToChunkServer(String info)
	{	
		Socket msgSocket = null;
		
		readFailedNodeInfo = info.split("#")[1].split(":")[0] + ":" + info.split("#")[1].split(":")[1];
		
//		if(readFailedNodeInfo == "")
//			readFailedNodeInfo += info.split("#")[1].split(":")[0] + ":" + info.split("#")[1].split(":")[1];
//		else
//			readFailedNodeInfo += "," + info.split("#")[1].split(":")[0] + ":" + info.split("#")[1].split(":")[1];
		
		try {
			System.out.println("Sending message to: " + InetAddress.getByName(info.split("#")[1].split(":")[0]) + " and " +  Integer.parseInt(info.split("#")[1].split(":")[1]));
		} catch (NumberFormatException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (UnknownHostException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		try {
			msgSocket = new Socket(InetAddress.getByName(info.split("#")[1].split(":")[0]), Integer.parseInt(info.split("#")[1].split(":")[1]));
		} catch (IOException e) {
			e.printStackTrace();
		};
		
		try {
			TCPSender sender =  new TCPSender(msgSocket);
			ReadChunkServerRequest readChnkServReq= new ReadChunkServerRequest();
			ownServerPort = server.getOwnPort();
			byte[] dataToSend = readChnkServReq.readFileChunkServerRequest(ownIP, ownServerPort, info.split("#")[0]);
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
	
	private void startMergingFiles() throws IOException, InterruptedException
	{
		List<File> files = new ArrayList<File>();
		
		for(int i = 1; i <= numOfFilesToBeMerged; i++)
		{
			files.add(new File("./FileStorage/" + readfileName + ".00" + i));
		}
		
		try (FileOutputStream fos = new FileOutputStream("./FileStorage/" + readfileName);
		    BufferedOutputStream mergingStream = new BufferedOutputStream(fos)) {
		        for (File f : files) {
		            Files.copy(f.toPath(), mergingStream);
		        }
			}
		
		numOfFilesToBeMerged = 0;
		
		// Removing the individual chunks
		Runtime run = Runtime.getRuntime();  
		Process p = null;  
		String cmd = "rm ";  
		try {
			for(int i = 0; i < files.size(); i++)
			{
				p = run.exec(cmd + files.get(i));  
				p.getErrorStream();  
				p.waitFor();
			}
		}  
		catch (IOException e) {  
		    e.printStackTrace();  
		    System.out.println("ERROR.RUNNING.CMD");  

		}finally{
		    p.destroy();
		}  
	}
	
	private void requestForAnotherReplica(String fileName)
	{
		Socket msgSocket = null;
		failureFlag = false;
		
		// Sending message to Controller asking for replication nodes
		try {
			msgSocket = new Socket(controllerNodeHost, controllerNodePort);
		} catch (IOException e) {
			e.printStackTrace();
		};
		
		//sending StoreFileControllerRequest message
		try {
			TCPSender sender =  new TCPSender(msgSocket);
			ReadFailedControllerRequest readFailedContReq= new ReadFailedControllerRequest();
			ownServerPort = server.getOwnPort();
			byte[] dataToSend = readFailedContReq.readFailedControllerRequest(ownIP, ownServerPort, fileName + "#" + readFailedNodeInfo);
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
	
	@Override
	public void onEvent(Event e) 
	{	
		int messageType = e.getMessageType();
		System.out.println("OnEvent messageType " + messageType);
		
		if(messageType == Protocol.STORE_FILE_CONTROLLER_RESPONSE)
		{
			System.out.println(e.getInfo());
			sendChunksToChunkServer(e.getInfo());
		}
		else if(messageType == Protocol.STORE_FILE_CHUNK_SERVER_RESPONSE)
		{
			System.out.println(e.getStatusCode());
			if((e.getStatusCode() == Protocol.SUCCESS) && (m_numOfChunkPartitions <= numPartitions))
			{
				handleFileStorage();
			}
		}
		else if(messageType == Protocol.READ_FILE_CONTROLLER_RESPONSE)
		{
			System.out.println(e.getInfo());
			numReadFilePartitions += 1;
			sendReadRequestToChunkServer(e.getInfo());
		}
		else if(messageType == Protocol.READ_FAILED_CONTROLLER_RESPONSE)
		{
			sendReadRequestToChunkServer(e.getInfo());
		}
		else if(messageType == Protocol.READ_FILE_CHUNK_SERVER_RESPONSE)
		{
			numOfFilesToBeMerged += 1;
			System.out.println("numOfFilesToBeMerged: " + numOfFilesToBeMerged + " numReadFilePartitions: " + numReadFilePartitions);
			if((e.getStatusCode() == Protocol.SUCCESS) && (numOfFilesToBeMerged ==  numReadFilePartitions))
			{
				if(!failureFlag)
				{
					try {
						startMergingFiles();
					} catch (IOException e1) {
						e1.printStackTrace();
					} catch (InterruptedException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}
			}
			else if(e.getStatusCode() == Protocol.FAILURE)
			{
				// Requesting Controller to send another replica to get the correct chunk file
				failureFlag = true;
				numOfFilesToBeMerged = numOfFilesToBeMerged - 1;
				requestForAnotherReplica(e.getInfo());
			}
			
		}
		
	}

	
	public static void main(String[] args) 
	{	
		// Starting the client nodes
		Client client = new Client();
		client.Initialize(args);
	}


}
