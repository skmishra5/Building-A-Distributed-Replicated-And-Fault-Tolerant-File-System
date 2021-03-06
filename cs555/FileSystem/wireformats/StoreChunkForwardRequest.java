package cs555.FileSystem.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Date;

public class StoreChunkForwardRequest implements Event{
	private int type;
	private long timestamp;
	private String m_filePath;
	private String m_fileName;
	private String m_info;
	private String IPAddress;
	private int listenPortNumber;
	private int forwardFlag = -1;

	public void setMessageType(int messageType){ type = messageType; }
	public int getMessageType(){ return type; }
	public int getNodeId(){ return -1;}
	public String getIPAddress(){ return IPAddress; }
	public int getLocalPortNumber(){ return -1;}
	public int getListenPortNumber(){ return listenPortNumber;}
	public String getNodeNickName(){ return null; }
	public byte getStatusCode(){ return 0; }
	public String getInfo(){ return m_fileName + "%" + m_info; }
	public int getForwardFlag(){ return forwardFlag; }
	public int getFingerTableEntry(){ return -1; }
	
	public byte[] chunkForwardRequest(String filePath, String fileName, String info, String IP, int listenPort, int frwdFlag) throws IOException
	{
		type = Protocol.STORE_CHUNK_FORWARD_REQUEST;
		Date dte=new Date();
	    timestamp = dte.getTime();
	    m_filePath = filePath;
	    m_fileName = fileName;
	    m_info = info;
	    IPAddress = IP;
	    listenPortNumber = listenPort;
	    forwardFlag = frwdFlag;
		byte[] marshalledBytes = getBytes();
		return marshalledBytes;
	}
	
	@Override
	public void getType(byte[] marshalledBytes) throws IOException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		int bytesRead;
	    int current = 0;
	    FileOutputStream fos = null;
	    BufferedOutputStream bos = null;

		type = din.readInt();
		timestamp = din.readLong();
		
		int identifierLength1 = din.readInt();
		byte[] identifierBytes1 = new byte[identifierLength1];
		din.readFully(identifierBytes1);
		m_fileName = new String(identifierBytes1);
		
		int fileSize = din.readInt();

		byte [] mybytearray  = new byte [fileSize];
	    fos = new FileOutputStream("/tmp/skmishra/" + m_fileName);
	    bos = new BufferedOutputStream(fos);
	    din.readFully(mybytearray);

	    bos.write(mybytearray, 0 , fileSize);
	    bos.flush();
		
		int identifierLength = din.readInt();
		byte[] identifierBytes = new byte[identifierLength];
		din.readFully(identifierBytes);
		IPAddress = new String(identifierBytes);

		listenPortNumber = din.readInt();
		
		int identifierLength2 = din.readInt();
		byte[] identifierBytes2 = new byte[identifierLength2];
		din.readFully(identifierBytes2);
		m_info = new String(identifierBytes2);
		
		forwardFlag = din.readInt();
		
		baInputStream.close();
		din.close();		
	}
	
	@Override
	public byte[] getBytes() throws IOException {		
		byte[] marshalledBytes = null;
		ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutputStream));
		File myFile = new File(m_filePath);
		byte [] mybytearray  = new byte [(int)myFile.length()];

		dout.writeInt(type);
		dout.writeLong(timestamp);
		
		byte[] identifierBytes1 = m_fileName.getBytes();
		int elementLength1 = identifierBytes1.length;
		dout.writeInt(elementLength1);
		dout.write(identifierBytes1);
				
		FileInputStream fis = new FileInputStream(myFile);
		BufferedInputStream bis = new BufferedInputStream(fis);
		bis.read(mybytearray, 0, mybytearray.length);
		dout.writeInt(mybytearray.length);
		dout.write(mybytearray);

		byte[] identifierBytes = IPAddress.getBytes();
		int elementLength = identifierBytes.length;
		dout.writeInt(elementLength);
		dout.write(identifierBytes);
		
		dout.writeInt(listenPortNumber);
		
		byte[] identifierBytes2 = m_info.getBytes();
		int elementLength2 = identifierBytes2.length;
		dout.writeInt(elementLength2);
		dout.write(identifierBytes2);
		
		dout.writeInt(forwardFlag);

		dout.flush();
		marshalledBytes = baOutputStream.toByteArray();
		baOutputStream.close();
		dout.close();
		return marshalledBytes;
	}
}
