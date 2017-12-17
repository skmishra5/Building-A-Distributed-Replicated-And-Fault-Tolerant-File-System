package cs555.FileSystem.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Date;

public class StoreChunkForwardResponse implements Event{
	private int type;
	private long timestamp;
	private int forwardFlag;
	private byte m_status;

	public void setMessageType(int messageType){ type = messageType; }
	public int getMessageType(){ return type; }
	public String getIPAddress(){ return null; }
	public int getListenPortNumber(){ return -1;}
	public byte getStatusCode(){ return m_status; }
	public String getInfo(){ return null; }
	public int getForwardFlag(){ return forwardFlag; }
	public int getFingerTableEntry(){ return -1; }
	
	public byte[] storeChunkForwardResponse(byte status, int frwfFlg) throws IOException
	{
		type = Protocol.STORE_CHUNK_FORWARD_RESPONSE;
		Date dte=new Date();
	    timestamp = dte.getTime();
	    forwardFlag = frwfFlg;
	    m_status = status;
		byte[] marshalledBytes = getBytes();
		return marshalledBytes;
	}
	
	@Override
	public void getType(byte[] marshalledBytes) throws IOException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));

		type = din.readInt();
		timestamp = din.readLong();
		forwardFlag = din.readInt();
		m_status = din.readByte();
		
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
		dout.writeInt(forwardFlag);
		dout.writeByte(m_status);
		
		dout.flush();
		marshalledBytes = baOutputStream.toByteArray();
		baOutputStream.close();
		dout.close();
		return marshalledBytes;
	}
}
