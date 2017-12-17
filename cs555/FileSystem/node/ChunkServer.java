package cs555.FileSystem.node;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import cs555.FileSystem.transport.CommandThread;
import cs555.FileSystem.transport.TCPSender;
import cs555.FileSystem.transport.TCPServerThread;
import cs555.FileSystem.util.MajorHeartbeat;
import cs555.FileSystem.util.MinorHeartbeat;
import cs555.FileSystem.wireformats.ErrorCorrFromChunkServerRequest;
import cs555.FileSystem.wireformats.Event;
import cs555.FileSystem.wireformats.EventFactory;
import cs555.FileSystem.wireformats.FailureDetectionResponse;
import cs555.FileSystem.wireformats.MajorHeartbeatMessage;
import cs555.FileSystem.wireformats.NewReplicaChunkServerRequest;
import cs555.FileSystem.wireformats.NewReplicaChunkServerResponse;
import cs555.FileSystem.wireformats.NewReplicationMessage;
import cs555.FileSystem.wireformats.Protocol;
import cs555.FileSystem.wireformats.ReadChunkServerResponse;
import cs555.FileSystem.wireformats.ReplicaCorrectionRequest;
import cs555.FileSystem.wireformats.ReplicaCorrectionResponse;
import cs555.FileSystem.wireformats.StoreChunkForwardRequest;
import cs555.FileSystem.wireformats.StoreChunkForwardResponse;
import cs555.FileSystem.wireformats.StoreFileChunkServerRequest;
import cs555.FileSystem.wireformats.StoreFileChunkServerResponse;

public class ChunkServer implements Node{

	private static String ownIP = "";
	private static int ownServerPort = -1;
	private InetAddress controllerNodeHost = null;
	private int controllerNodePort = -1;
	private EventFactory eventFactory;
	public static TCPServerThread server;
	private Thread serverThread;
	private CommandThread commandInput;
	private MajorHeartbeat majorHeartbeat;
	private MinorHeartbeat minorHeartbeat;
	private static ArrayList<String> chunkFileNames = new ArrayList<String>();
	public static HashMap<String, String> chunkInformation = new HashMap<String, String>();
	private String firstChunkServerIP = "";
	private int firstChunkServerPort = -1;
	private String clientIP = "";
	private int clientPort = -1;
	private int decision = -1;
	private String corruptedChunkFile = "";
	
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
		
		//ownServerPort = server.getOwnPort();
		
		commandInput = new CommandThread(2);
		Thread commandThread = new Thread(commandInput);
		commandThread.start();
		
		// Setting up major heat-beat thread
		//majorHeartbeat = new MajorHeartbeat(this, controllerNodeHost, controllerNodePort, ownIP);
		//Thread majorHeartbeatThread = new Thread(majorHeartbeat);
		//majorHeartbeatThread.start();
		
		// Setting up major heat-beat thread
		minorHeartbeat = new MinorHeartbeat(this, controllerNodeHost, controllerNodePort, ownIP);
		Thread minorHeartbeatThread = new Thread(minorHeartbeat);
		minorHeartbeatThread.start();
	}
	
	private void storeChecksumInfo(String fileName) throws FileNotFoundException, IOException, NoSuchAlgorithmException
	{
		int sizeOfFiles = 1024 * 8;// 8KB
		byte[] buffer = new byte[sizeOfFiles];
		
		//String[] token = fileName.split("/");

		//try-with-resources to ensure closing stream
		try (FileInputStream fis = new FileInputStream("/tmp/skmishra/" + fileName);
		BufferedInputStream bis = new BufferedInputStream(fis)) 
		{
			int bytesAmount = 0;
			while ((bytesAmount = bis.read(buffer)) > 0){				
				java.security.MessageDigest digest = null;
				digest = java.security.MessageDigest.getInstance("SHA-1");
				digest.update(buffer);
				byte[] hashedBytes = digest.digest();
				try (FileOutputStream out = new FileOutputStream(new File("/tmp/skmishra/" + fileName + ".Checksum"), true)) {
					out.write(hashedBytes);
				}
			}
		}
		
		System.out.println("Checksum file created for " + fileName);
	}
	
	private void storeMetaDataInfo(String filename) throws FileNotFoundException, IOException
	{
		int versionNumber = 1;
		int seqNumber = 1;
		String fileName = filename;
		Date dte=new Date();
	    long timestamp = dte.getTime();
	    
	    String content = versionNumber + ":" + seqNumber + ":" + fileName + ":" + timestamp;
	    byte[] contentBytes = content.getBytes();
	    
	    try (FileOutputStream out = new FileOutputStream(new File("/tmp/skmishra/" + fileName + ".Metadata"), true)) {
			try {
				out.write(contentBytes);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	    
	    chunkInformation.put(fileName, content);
	}
	
	private void sendChunkReplicationToOtherNode(String info, String fileName)
	{
		System.out.println("Inside sendChunkReplicationToOtherNode");
		
		Socket msgSocket = null;

		try {
			msgSocket = new Socket(InetAddress.getByName(info.split("#")[1].split(":")[0]), Integer.parseInt(info.split("#")[1].split(":")[1]));
		} catch (IOException e) {
			e.printStackTrace();
		};

		try {
			TCPSender sender =  new TCPSender(msgSocket);
			StoreChunkForwardRequest stchunkFrwdReq= new StoreChunkForwardRequest();
			ownServerPort = server.getOwnPort();
			byte[] dataToSend = stchunkFrwdReq.chunkForwardRequest("/tmp/skmishra/" + fileName, fileName, info.split("#")[2], ownIP, ownServerPort, 1);
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
	
	private void sendAcknowledgementForChunkFileStorage(String IP, int port)
	{
		Socket msgSocket = null;
		
		// Sending message to client as an acknowledgement of file storage
		try {
			msgSocket = new Socket(InetAddress.getByName(IP), port);
		} catch (IOException e) {
			e.printStackTrace();
		};

		try {
			TCPSender sender =  new TCPSender(msgSocket);
			StoreFileChunkServerResponse stFlChnkSrvRes= new StoreFileChunkServerResponse();
			byte[] dataToSend = stFlChnkSrvRes.storeFileChunkServerResponse(Protocol.SUCCESS);
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
	
	private void sendChunkFileToLastReplicationNode(String info, String fileName)
	{
		System.out.println("Inside sendChunkFileToLastReplicationNode");
		Socket msgSocket = null;
		
		// Sending message to Chunk Server to store chunks
		try {
			msgSocket = new Socket(InetAddress.getByName(info.split(":")[0]), Integer.parseInt(info.split(":")[1]));
		} catch (IOException e) {
			e.printStackTrace();
		};
		
		//sending StoreFileControllerRequest message
		try {
			TCPSender sender =  new TCPSender(msgSocket);
			StoreChunkForwardRequest stchunkFrwdReq= new StoreChunkForwardRequest();
			ownServerPort = server.getOwnPort();
			byte[] dataToSend = stchunkFrwdReq.chunkForwardRequest("/tmp/skmishra/" + fileName, fileName, info, ownIP, ownServerPort, 0);
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
	
	private void sendChunkForwardResponseMessage(String IP, int port, int flag)
	{
		Socket msgSocket = null;
		
		// Sending message to Chunk Server to store chunks
		try {
			msgSocket = new Socket(InetAddress.getByName(IP), port);
		} catch (IOException e) {
			e.printStackTrace();
		};
		
		//sending StoreFileControllerRequest message
		try {
			TCPSender sender =  new TCPSender(msgSocket);
			StoreChunkForwardResponse stchunkFrwdRes= new StoreChunkForwardResponse();
			byte[] dataToSend = stchunkFrwdRes.storeChunkForwardResponse(Protocol.SUCCESS, flag);
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
	
	private void sendChunkFilesToClient(String clientIP, int clientPort, String fileName)
	{		
		System.out.println("Inside sendChunkFilesToClient");
		
		// Check for integrity of the chunk file
		try {
			decision = checkIntegrityOfChunkFile(fileName);
			System.out.println("Decision at First: " + decision);
			if(decision == 0)
			{
				// Sending client the chunk file
				Socket msgSocket = null;

				try {
					msgSocket = new Socket(InetAddress.getByName(clientIP), clientPort);
				} catch (IOException e) {
					e.printStackTrace();
				};

				try {
					TCPSender sender =  new TCPSender(msgSocket);
					ReadChunkServerResponse rdChnkServRes= new ReadChunkServerResponse();
					byte[] dataToSend = rdChnkServRes.readChunkServerResponseMessage("/tmp/skmishra/" + fileName, fileName, Protocol.SUCCESS);
					sender.sendData(dataToSend);
				} catch (IOException e) {
					e.printStackTrace();
				}
				
				try {
					msgSocket.close();
					msgSocket = null;
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}			
			}
			else
			{
				// Sending client a read failure message
				Socket msgSocket = null;

				try {
					msgSocket = new Socket(InetAddress.getByName(clientIP), clientPort);
				} catch (IOException e) {
					e.printStackTrace();
				};

				try {
					TCPSender sender =  new TCPSender(msgSocket);
					ReadChunkServerResponse rdChnkServRes= new ReadChunkServerResponse();
					byte[] dataToSend = rdChnkServRes.readChunkServerResponseMessage(null, fileName, Protocol.FAILURE);
					sender.sendData(dataToSend);
				} catch (IOException e) {
					e.printStackTrace();
				}
				
				try {
					msgSocket.close();
					msgSocket = null;
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				// Start correcting the file
				corruptedChunkFile = fileName;
				startChunkFileErrorCorrection(fileName);
			}
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void startChunkFileErrorCorrection(String fileName)
	{
		// Sending message to Controller to send other replica information
		Socket msgSocket = null;

		try {
			msgSocket = new Socket(controllerNodeHost, controllerNodePort);
		} catch (IOException e) {
			e.printStackTrace();
		};
		try {
			TCPSender sender =  new TCPSender(msgSocket);
			ErrorCorrFromChunkServerRequest errCorrFrmChnkServReq= new ErrorCorrFromChunkServerRequest();
			ownServerPort = server.getOwnPort();
			byte[] dataToSend = errCorrFrmChnkServReq.errorCorrFromChunkServerRequest(ownIP, ownServerPort, fileName);
			sender.sendData(dataToSend);
		} catch (IOException e) {
			e.printStackTrace();
		}		
		try {
			msgSocket.close();
			msgSocket = null;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}			
	}
	
	private int checkIntegrityOfChunkFile(String fileName) throws FileNotFoundException, IOException, NoSuchAlgorithmException
	{
		int sizeOfFiles = 1024 * 8;// 8KB
		byte[] buffer = new byte[sizeOfFiles];
		ArrayList<String> temp = new ArrayList<String>();

		//try-with-resources to ensure closing stream
		try (FileInputStream fis = new FileInputStream("/tmp/skmishra/" + fileName);
		BufferedInputStream bis = new BufferedInputStream(fis)) 
		{
			int bytesAmount = 0;
			while ((bytesAmount = bis.read(buffer)) > 0){				
				java.security.MessageDigest digest = null;
				digest = java.security.MessageDigest.getInstance("SHA-1");
				digest.update(buffer);
				byte[] hashedBytes = digest.digest();
				temp.add(convertByteArrayToHexString(hashedBytes));
			}
		}
		
		byte[] buffer1 = new byte[20];
		int corruptedSliceNumber = 0;
		
		//try-with-resources to ensure closing stream
		try (FileInputStream fis = new FileInputStream("/tmp/skmishra/" + fileName + ".Checksum");
		BufferedInputStream bis = new BufferedInputStream(fis)) 
		{
			int bytesAmount1 = 0;
			int counter = 0;
			while ((bytesAmount1 = bis.read(buffer1)) > 0){				
				String readTemp = convertByteArrayToHexString(buffer1);
				if(counter < temp.size())
				{
					if(!(temp.get(counter).trim().equals(readTemp.trim())))
					{
						corruptedSliceNumber = counter + 1;
						return corruptedSliceNumber;
					}
				}
				counter++;
			}
		}
		
		return corruptedSliceNumber;
	}
	
	public static String convertByteArrayToHexString(byte[] arrayBytes) {
		 StringBuffer stringBuffer = new StringBuffer();
		 for (int i = 0; i < arrayBytes.length; i++) {
		     stringBuffer.append(Integer.toString((arrayBytes[i] & 0xff) + 0x100, 16)
		             .substring(1));
		 }
		 return stringBuffer.toString();
	}
	
	private void requestReplicationFromOtherChunkServer(String info)
	{
		Socket msgSocket = null;

		try {
			msgSocket = new Socket(InetAddress.getByName(info.split("#")[1].split(":")[0]), Integer.parseInt(info.split("#")[1].split(":")[1]));
		} catch (IOException e) {
			e.printStackTrace();
		};
		try {
			TCPSender sender =  new TCPSender(msgSocket);
			ReplicaCorrectionRequest repCorrReq= new ReplicaCorrectionRequest();
			ownServerPort = server.getOwnPort();
			System.out.println("Decision at Second: " + decision);
			byte[] dataToSend = repCorrReq.replicaCorrectionRequest(ownIP, ownServerPort, info.split("#")[0] + "#" + decision);
			sender.sendData(dataToSend);
		} catch (IOException e) {
			e.printStackTrace();
		}		
		try {
			msgSocket.close();
			msgSocket = null;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void handleReplicaCorrection(String sendBackIP, int sendBackPort, String info) throws FileNotFoundException, IOException
	{
		String fileName = info.split("#")[0];
		int numOfSlices = Integer.parseInt(info.split("#")[1]);
		System.out.println("Decision at third: " + numOfSlices);
		
		int sizeOfFiles = 1024 * 8;// 8KB
		byte[] buffer = new byte[sizeOfFiles];

		//try-with-resources to ensure closing stream
		try (FileInputStream fis = new FileInputStream("/tmp/skmishra/" + fileName);
		BufferedInputStream bis = new BufferedInputStream(fis)) 
		{
			int bytesAmount = 0;
			int counter = 0;
			boolean flag = false;
			while ((bytesAmount = bis.read(buffer)) > 0){				
				counter++;
				System.out.println("Value of counter " + counter);
				if((counter == numOfSlices) || (flag == true))
				{
					try (FileOutputStream out = new FileOutputStream(new File("/tmp/skmishra/" + fileName + ".tmp"), true)) {
						out.write(buffer);
					}
					flag = true;
				}
			}
		}
		
		// Sending the required slices back to the corrupted replica node
		Socket msgSocket = null;

		try {
			msgSocket = new Socket(InetAddress.getByName(sendBackIP), sendBackPort);
		} catch (IOException e) {
			e.printStackTrace();
		};
		try {
			TCPSender sender =  new TCPSender(msgSocket);
			ReplicaCorrectionResponse repCorrRes= new ReplicaCorrectionResponse();
			byte[] dataToSend = repCorrRes.replicaCorrectionResponseMessage("/tmp/skmishra/" + fileName + ".tmp", fileName, Protocol.SUCCESS);
			sender.sendData(dataToSend);
		} catch (IOException e) {
			e.printStackTrace();
		}		
		try {
			msgSocket.close();
			msgSocket = null;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void correctCorruption(String fileName) throws FileNotFoundException, IOException
	{	
		System.out.println("Tempo file: " + fileName + "and corruptedChunkFile: " + corruptedChunkFile);
		RandomAccessFile f = new RandomAccessFile("/tmp/skmishra/" + fileName + ".tmp", "r");
		int lengthOfTempFile = (int)f.length();
		byte[] b = new byte[lengthOfTempFile];
		f.readFully(b);
		
		int offset = (decision * 8) - 8;
		System.out.println("Value of decision and offset: " + decision + " " + offset);
		try (FileOutputStream fos = new FileOutputStream("/tmp/skmishra/" + corruptedChunkFile);
			    BufferedOutputStream mergingStream = new BufferedOutputStream(fos)) {
					mergingStream.write(b, offset, lengthOfTempFile);
				}
		
		corruptedChunkFile = "";
	}
	
	private void handleFailureDetectionMessage()
	{
		// Reply Controller with the list of files
		Socket msgSocket = null;

		try {
			msgSocket = new Socket(controllerNodeHost, controllerNodePort);
		} catch (IOException e) {
			e.printStackTrace();
		};
		try {
			TCPSender sender =  new TCPSender(msgSocket);
			FailureDetectionResponse failDetRes= new FailureDetectionResponse();
			ownServerPort = server.getOwnPort();
			String info = "";
			int count = 0;
			for(Map.Entry<String, String> entry : chunkInformation.entrySet())
			{
				count++;
				if(count == chunkInformation.size())
					info += entry.getKey();
				else
					info += entry.getKey() + ",";
			}
			byte[] dataToSend = failDetRes.failureDetectionResponse(ownIP, ownServerPort, info);
			sender.sendData(dataToSend);
		} catch (IOException e) {
			e.printStackTrace();
		}		
		try {
			msgSocket.close();
			msgSocket = null;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void handleNewReplication(String sendIP, int sendPort, String fileName)
	{
		for(int i = 0; i < 3; i++)
		{
			String newFileName = ""; 
			if(i == 0)
				newFileName = fileName;
			else if(i == 1)
				newFileName = fileName + ".Checksum";
			else if(i == 2)
				newFileName = fileName + ".Metadata";
			
			Socket msgSocket = null;
			try {
				msgSocket = new Socket(InetAddress.getByName(sendIP), sendPort);
			}
			catch (IOException e1) {
				e1.printStackTrace();
			};
		
			try {
				TCPSender sender =  new TCPSender(msgSocket);
				NewReplicaChunkServerResponse newReplChnkRes= new NewReplicaChunkServerResponse();
				byte[] dataToSend = newReplChnkRes.newReplicaChunkServerResponse("/tmp/skmishra/" + newFileName , newFileName);
				sender.sendData(dataToSend);
			}
			catch (IOException e2) {
				e2.printStackTrace();
			}
		
			try {
				msgSocket.close();
			} catch (IOException e3) {
				// TODO Auto-generated catch block
				e3.printStackTrace();
			}
		}
	}
	
	@Override
	public void onEvent(Event e) 
	{
		int messageType = e.getMessageType();
		System.out.println("OnEvent messageType " + messageType);
		
		if(messageType == Protocol.STORE_FILE_CHUNK_SERVER_REQUEST)
		{
			System.out.println("File Stored: " +  e.getInfo());
			if(!chunkFileNames.contains(e.getInfo().split("%")[0]))
			{
				chunkFileNames.add(e.getInfo().split("%")[0]);
			}
			try {
				storeChecksumInfo(e.getInfo().split("%")[0]);
			} catch (FileNotFoundException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (NoSuchAlgorithmException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			try {
				storeMetaDataInfo(e.getInfo().split("%")[0]);
			} catch (FileNotFoundException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			sendChunkReplicationToOtherNode(e.getInfo().split("%")[1], e.getInfo().split("%")[0]);
			clientIP = e.getIPAddress();
			clientPort = e.getListenPortNumber();
		}
		else if(messageType == Protocol.STORE_CHUNK_FORWARD_REQUEST)
		{
			System.out.println("Replication file Stored: " +  e.getInfo());
			if(!chunkFileNames.contains(e.getInfo().split("%")[0]))
			{
				chunkFileNames.add(e.getInfo().split("%")[0]);
			}
			try {
				storeChecksumInfo(e.getInfo().split("%")[0]);
			} catch (FileNotFoundException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (NoSuchAlgorithmException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			try {
				storeMetaDataInfo(e.getInfo().split("%")[0]);
			} catch (FileNotFoundException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
			if(e.getForwardFlag() == 1)
			{
				firstChunkServerIP = e.getIPAddress();
				firstChunkServerPort = e.getListenPortNumber();
				sendChunkFileToLastReplicationNode(e.getInfo().split("%")[1], e.getInfo().split("%")[0]);
			}
			else
			{
				sendChunkForwardResponseMessage(e.getIPAddress(), e.getListenPortNumber(), 1);
			}
		}
		else if(messageType == Protocol.STORE_CHUNK_FORWARD_RESPONSE)
		{
			if(e.getForwardFlag() == 1)
			{
				sendChunkForwardResponseMessage(firstChunkServerIP, firstChunkServerPort, 0);
			}
			else
			{
				sendAcknowledgementForChunkFileStorage(clientIP, clientPort);
			}
		}
		else if(messageType == Protocol.READ_FILE_CHUNK_SERVER_REQUEST)
		{
			sendChunkFilesToClient(e.getIPAddress(), e.getListenPortNumber(), e.getInfo());
		}
		else if(messageType == Protocol.ERROR_CORRECTION_FROM_CHUNKSERVER_RESPONSE)
		{
			requestReplicationFromOtherChunkServer(e.getInfo());
		}
		else if(messageType == Protocol.REPLICA_CORRECTION_REQUEST)
		{
			try {
				handleReplicaCorrection(e.getIPAddress(), e.getListenPortNumber(), e.getInfo());
			} catch (FileNotFoundException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
		else if(messageType == Protocol.REPLICA_CORRECTION_RESPONSE)
		{
			try {
				correctCorruption(e.getInfo());
			} catch (FileNotFoundException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
		else if(messageType == Protocol.FAILURE_DETECTION_REQUEST)
		{
			handleFailureDetectionMessage();
		}
		else if(messageType == Protocol.NEW_REPLICATION_MESSAGE)
		{
			Socket msgSocket = null;
			try {
				msgSocket = new Socket(InetAddress.getByName(e.getInfo().split("#")[1].split(":")[0]), Integer.parseInt(e.getInfo().split("#")[1].split(":")[1]));
			}
			catch (IOException e1) {
				e1.printStackTrace();
			};
			
			try {
				TCPSender sender =  new TCPSender(msgSocket);
				NewReplicaChunkServerRequest newReplChnkReq= new NewReplicaChunkServerRequest();
				byte[] dataToSend = newReplChnkReq.newReplicaChunkServerRequest(ownIP, ownServerPort, e.getInfo().split("#")[0]);
				sender.sendData(dataToSend);
			}
			catch (IOException e2) {
				e2.printStackTrace();
			}
			
			try {
				msgSocket.close();
			} catch (IOException e3) {
				// TODO Auto-generated catch block
				e3.printStackTrace();
			}
		}
		else if(messageType == Protocol.NEW_REPLICA_CHUNKSERVER_MESSAGE)
		{
			handleNewReplication(e.getIPAddress(), e.getListenPortNumber(), e.getInfo());
		}
		else if(messageType == Protocol.NEW_REPLICA_CHUNKSERVER_RESPONSE)
		{
			if(!chunkFileNames.contains(e.getInfo().trim()))
			{
				chunkFileNames.add(e.getInfo().trim());
			}
			
			if(e.getInfo().endsWith("Metadata"))
			{
				FileInputStream fstream = null;
				try {
					fstream = new FileInputStream("/tmp/skmishra/" + e.getInfo());
				} catch (FileNotFoundException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
				String strLine;
				String content = "";
				String fileName = "";

				//Read File Line By Line
				try {
					while ((strLine = br.readLine()) != null)   {
						// Print the content on the console
						content = strLine;
						fileName = content.split(":")[2];
					}
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				
				}
				//Close the input stream
				try {
					br.close();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				chunkInformation.put(fileName, content);
			}
		}
		else if(messageType == Protocol.REQUEST_MAJOR_HEARTBEAT)
		{
			String mapout = "";
			if(!chunkInformation.isEmpty()){
				for(Map.Entry<String, String> entry : chunkInformation.entrySet())
				{
					mapout += entry.getKey() + "#" + entry.getValue() + ",";
				}
			}
			else
			{
				mapout = "NONE";
			}
			
			Socket msgSocket = null;
			
			try {
				msgSocket = new Socket(controllerNodeHost, controllerNodePort);
			} catch (IOException e1) {
				e1.printStackTrace();
			};
			
			try {
				TCPSender sender =  new TCPSender(msgSocket);
				ownServerPort = server.getOwnPort();
				MajorHeartbeatMessage majHrtbtMsg= new MajorHeartbeatMessage();
				byte[] dataToSend = majHrtbtMsg.majorHeartbeatMessage(ownIP, ownServerPort, mapout);
				sender.sendData(dataToSend);
			} catch (IOException e2) {
				e2.printStackTrace();
			}
			
			try {
				msgSocket.close();
			} catch (IOException e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			}
			
			mapout = "";
		}
		
	}
	public void printChunkInfo()
	{
		for(Map.Entry<String, String> entry : chunkInformation.entrySet())
		{
			System.out.println(entry.getKey() + "$" + entry.getValue() + ",");
		}
	}
	
	
	public static void main(String[] args) 
	{	
		// Starting the client nodes
		ChunkServer chunkServer = new ChunkServer();
		chunkServer.Initialize(args);
	}

}
