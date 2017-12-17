package cs555.FileSystem.wireformats;

import java.io.IOException;

public interface Event {
	// Methods to Marshall and Unmarshall messages
	public void getType(byte[] marshalledBytes)
		throws IOException;
	public byte[] getBytes()
		throws IOException;
	
	//Method to set and get Message Type
	public void setMessageType(int messageType);
	public int getMessageType();
	public String getIPAddress();
	public int getListenPortNumber();
	public byte getStatusCode();
	public String getInfo();
	public int getForwardFlag();
	public int getFingerTableEntry();
}
