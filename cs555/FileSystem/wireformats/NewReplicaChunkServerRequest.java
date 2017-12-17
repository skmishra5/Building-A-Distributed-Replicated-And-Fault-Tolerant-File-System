package cs555.FileSystem.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Date;

public class NewReplicaChunkServerRequest implements Event{
	private int type;
	private long timestamp;
	private String IPAddress;
	private int listenPortNumber;
	private String info;

	public void setMessageType(int messageType){ type = messageType; }
	public int getMessageType(){ return type; }
	public String getIPAddress(){ return IPAddress; }
	public int getListenPortNumber(){ return listenPortNumber;}
	public byte getStatusCode(){ return 0; }
	public String getInfo(){ return info; }
	public int getForwardFlag(){ return -1; }
	public int getFingerTableEntry(){ return -1; }
	
	public byte[] newReplicaChunkServerRequest(String IP, int listenPort, String inf) throws IOException
	{
		type = Protocol.NEW_REPLICA_CHUNKSERVER_MESSAGE;
		Date dte=new Date();
	    timestamp = dte.getTime();
		IPAddress = IP;
		listenPortNumber = listenPort;
		info = inf;
		byte[] marshalledBytes = getBytes();
		return marshalledBytes;
	}
	
	@Override
	public void getType(byte[] marshalledBytes) throws IOException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));

		type = din.readInt();
		timestamp = din.readLong();
		
		int identifierLength = din.readInt();
		byte[] identifierBytes = new byte[identifierLength];
		din.readFully(identifierBytes);
		IPAddress = new String(identifierBytes);

		listenPortNumber = din.readInt();
		
		int identifierLength1 = din.readInt();
		byte[] identifierBytes1 = new byte[identifierLength1];
		din.readFully(identifierBytes1);
		info = new String(identifierBytes1);
		
		baInputStream.close();
		din.close();		
	}
	
	@Override
	public byte[] getBytes() throws IOException {		
		byte[] marshalledBytes = null;
		ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutputStream));

		dout.writeInt(type);
		dout.writeLong(timestamp);
		
		byte[] identifierBytes = IPAddress.getBytes();
		int elementLength = identifierBytes.length;
		dout.writeInt(elementLength);
		dout.write(identifierBytes);
		
		dout.writeInt(listenPortNumber);
		
		byte[] identifierBytes1 = info.getBytes();
		int elementLength1 = identifierBytes1.length;
		dout.writeInt(elementLength1);
		dout.write(identifierBytes1);

		dout.flush();
		marshalledBytes = baOutputStream.toByteArray();
		baOutputStream.close();
		dout.close();
		return marshalledBytes;
	}
}
