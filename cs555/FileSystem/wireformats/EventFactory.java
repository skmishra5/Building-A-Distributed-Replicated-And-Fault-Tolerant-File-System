package cs555.FileSystem.wireformats;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import cs555.FileSystem.node.Node;


public class EventFactory {
	private static EventFactory instance = null;
	private Node node = null; 
	
	private EventFactory(){};
	
	public static EventFactory getInstance(){
		// Creating Singleton instance
		if(instance == null){
			instance = new EventFactory();
		}
		return instance;
	}
	
	public void setNodeInstance(Node n)
	{
		node = n;
	}
	
	public void processReceivedMessage(byte[] data) throws IOException
	{
		int messageType = getMessageType(data);
		System.out.println("Message Type: " + messageType);
		
		if(messageType == Protocol.STORE_FILE_CONTROLLER_REQUEST)
		{
			Event e = new StoreFileControllerRequest();
			e.getType(data);
			e.setMessageType(messageType);
			node.onEvent(e);
		}
		else if(messageType == Protocol.STORE_FILE_CONTROLLER_RESPONSE)
		{
			Event e = new StoreFileControllerResponse();
			e.getType(data);
			e.setMessageType(messageType);
			node.onEvent(e);
		}
		else if(messageType == Protocol.STORE_FILE_CHUNK_SERVER_REQUEST)
		{
			Event e = new StoreFileChunkServerRequest();
			e.getType(data);
			e.setMessageType(messageType);
			node.onEvent(e);
		}
		else if(messageType == Protocol.STORE_CHUNK_FORWARD_REQUEST)
		{
			Event e = new StoreChunkForwardRequest();
			e.getType(data);
			e.setMessageType(messageType);
			node.onEvent(e);
		}
		else if(messageType == Protocol.STORE_FILE_CHUNK_SERVER_RESPONSE)
		{
			Event e = new StoreFileChunkServerResponse();
			e.getType(data);
			e.setMessageType(messageType);
			node.onEvent(e);
		}
		else if(messageType == Protocol.STORE_CHUNK_FORWARD_RESPONSE)
		{
			Event e = new StoreChunkForwardResponse();
			e.getType(data);
			e.setMessageType(messageType);
			node.onEvent(e);
		}
		else if(messageType == Protocol.MAJOR_HEARTBEAT_MESSAGE)
		{
			Event e = new MajorHeartbeatMessage();
			e.getType(data);
			e.setMessageType(messageType);
			node.onEvent(e);
		}
		else if(messageType == Protocol.MINOR_HEARTBEAT_MESSAGE)
		{
			Event e = new MinorHeartbeatMessage();
			e.getType(data);
			e.setMessageType(messageType);
			node.onEvent(e);
		}
		else if(messageType == Protocol.READ_FILE_CONTROLLER_REQUEST)
		{
			Event e = new ReadControllerRequest();
			e.getType(data);
			e.setMessageType(messageType);
			node.onEvent(e);
		}
		else if(messageType == Protocol.READ_FILE_CONTROLLER_RESPONSE)
		{
			Event e = new ReadControllerResponse();
			e.getType(data);
			e.setMessageType(messageType);
			node.onEvent(e);
		}
		else if(messageType == Protocol.READ_FILE_CHUNK_SERVER_REQUEST)
		{
			Event e = new ReadChunkServerRequest();
			e.getType(data);
			e.setMessageType(messageType);
			node.onEvent(e);
		}
		else if(messageType == Protocol.READ_FILE_CHUNK_SERVER_RESPONSE)
		{
			Event e = new ReadChunkServerResponse();
			e.getType(data);
			e.setMessageType(messageType);
			node.onEvent(e);
		}
		else if(messageType == Protocol.ERROR_CORRECTION_FROM_CHUNKSERVER_REQUEST)
		{
			Event e = new ErrorCorrFromChunkServerRequest();
			e.getType(data);
			e.setMessageType(messageType);
			node.onEvent(e);
		}
		else if(messageType == Protocol.ERROR_CORRECTION_FROM_CHUNKSERVER_RESPONSE)
		{
			Event e = new ErrorCorrFromChunkServerResponse();
			e.getType(data);
			e.setMessageType(messageType);
			node.onEvent(e);
		}
		else if(messageType == Protocol.REPLICA_CORRECTION_REQUEST)
		{
			Event e = new ReplicaCorrectionRequest();
			e.getType(data);
			e.setMessageType(messageType);
			node.onEvent(e);
		}
		else if(messageType == Protocol.REPLICA_CORRECTION_RESPONSE)
		{
			Event e = new ReplicaCorrectionResponse();
			e.getType(data);
			e.setMessageType(messageType);
			node.onEvent(e);
		}
		else if(messageType == Protocol.READ_FAILED_CONTROLLER_REQUEST)
		{
			Event e = new ReadFailedControllerRequest();
			e.getType(data);
			e.setMessageType(messageType);
			node.onEvent(e);
		}
		else if(messageType == Protocol.READ_FAILED_CONTROLLER_RESPONSE)
		{
			Event e = new ReadFailedControllerResponse();
			e.getType(data);
			e.setMessageType(messageType);
			node.onEvent(e);
		}
		else if(messageType == Protocol.FAILURE_DETECTION_REQUEST)
		{
			Event e = new FailureDetectionRequest();
			e.getType(data);
			e.setMessageType(messageType);
			node.onEvent(e);
		}
		else if(messageType == Protocol.FAILURE_DETECTION_RESPONSE)
		{
			Event e = new FailureDetectionResponse();
			e.getType(data);
			e.setMessageType(messageType);
			node.onEvent(e);
		}
		else if(messageType == Protocol.NEW_REPLICATION_MESSAGE)
		{
			Event e = new NewReplicationMessage();
			e.getType(data);
			e.setMessageType(messageType);
			node.onEvent(e);
		}
		else if(messageType == Protocol.NEW_REPLICA_CHUNKSERVER_MESSAGE)
		{
			Event e = new NewReplicaChunkServerRequest();
			e.getType(data);
			e.setMessageType(messageType);
			node.onEvent(e);
		}
		else if(messageType == Protocol.NEW_REPLICA_CHUNKSERVER_RESPONSE)
		{
			Event e = new NewReplicaChunkServerResponse();
			e.getType(data);
			e.setMessageType(messageType);
			node.onEvent(e);
		}
		else if(messageType == Protocol.REQUEST_MAJOR_HEARTBEAT)
		{
			Event e = new RequestMajorHeartbeat();
			e.getType(data);
			e.setMessageType(messageType);
			node.onEvent(e);
		}
	}
	
	private int getMessageType(byte[] data) throws IOException
	{
		int type;
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(data);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));

		type = din.readInt();
		baInputStream.close();
		din.close();
		return type;
	}
}
