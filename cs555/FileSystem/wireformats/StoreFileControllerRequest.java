package cs555.FileSystem.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Date;

public class StoreFileControllerRequest implements Event{
	private int type;
	private long timestamp;
	private String IPAddress;
	private int listenPortNumber;
	private String fileName;
	private int numPartitions;

	public void setMessageType(int messageType){ type = messageType; }
	public int getMessageType(){ return type; }
	public String getIPAddress(){ return IPAddress; }
	public int getListenPortNumber(){ return listenPortNumber;}
	public byte getStatusCode(){ return 0; }
	public String getInfo(){ return fileName; }
	public int getForwardFlag(){ return numPartitions; }
	public int getFingerTableEntry(){ return -1; }
	
	public byte[] storeFileControllerRequest(String IP, int listenPort, String flName, int numPart) throws IOException
	{
		type = Protocol.STORE_FILE_CONTROLLER_REQUEST;
		Date dte=new Date();
	    timestamp = dte.getTime();
		IPAddress = IP;
		listenPortNumber = listenPort;
		fileName = flName;
		numPartitions = numPart;
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
		fileName = new String(identifierBytes1);
		
		numPartitions = din.readInt();
		
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
		
		byte[] identifierBytes1 = fileName.getBytes();
		int elementLength1 = identifierBytes1.length;
		dout.writeInt(elementLength1);
		dout.write(identifierBytes1);
		
		dout.writeInt(numPartitions);

		dout.flush();
		marshalledBytes = baOutputStream.toByteArray();
		baOutputStream.close();
		dout.close();
		return marshalledBytes;
	}
}
