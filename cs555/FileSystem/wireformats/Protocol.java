package cs555.FileSystem.wireformats;

public class Protocol {
	// Declaring status code
	public static final byte SUCCESS = 0;
	public static final byte FAILURE = -1;
			
	// Declaring Message Types
	public static final int STORE_FILE_CONTROLLER_REQUEST = 0;
	public static final int STORE_FILE_CONTROLLER_RESPONSE = 1;
	public static final int STORE_FILE_CHUNK_SERVER_REQUEST = 2;
	public static final int STORE_CHUNK_FORWARD_REQUEST = 3;
	public static final int STORE_FILE_CHUNK_SERVER_RESPONSE = 4;
	public static final int STORE_CHUNK_FORWARD_RESPONSE = 5;
	public static final int MAJOR_HEARTBEAT_MESSAGE = 6;
	public static final int MINOR_HEARTBEAT_MESSAGE = 7;
	public static final int READ_FILE_CONTROLLER_REQUEST = 8;
	public static final int READ_FILE_CONTROLLER_RESPONSE = 9;
	public static final int READ_FILE_CHUNK_SERVER_REQUEST = 10;
	public static final int READ_FILE_CHUNK_SERVER_RESPONSE = 11;
	public static final int ERROR_CORRECTION_FROM_CHUNKSERVER_REQUEST = 12;
	public static final int ERROR_CORRECTION_FROM_CHUNKSERVER_RESPONSE = 13;
	public static final int REPLICA_CORRECTION_REQUEST = 14;
	public static final int REPLICA_CORRECTION_RESPONSE = 15;
	public static final int READ_FAILED_CONTROLLER_REQUEST = 16;
	public static final int READ_FAILED_CONTROLLER_RESPONSE = 17;
	public static final int FAILURE_DETECTION_REQUEST = 18;
	public static final int FAILURE_DETECTION_RESPONSE = 19;
	public static final int NEW_REPLICATION_MESSAGE = 20;
	public static final int NEW_REPLICA_CHUNKSERVER_MESSAGE = 21;
	public static final int NEW_REPLICA_CHUNKSERVER_RESPONSE = 22;
	public static final int REQUEST_MAJOR_HEARTBEAT = 23;
}
