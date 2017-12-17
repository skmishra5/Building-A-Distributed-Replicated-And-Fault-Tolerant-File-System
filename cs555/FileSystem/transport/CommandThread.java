package cs555.FileSystem.transport;

import java.util.Scanner;

import cs555.FileSystem.node.ChunkServer;
import cs555.FileSystem.node.Client;
import cs555.FileSystem.node.Controller;


public class CommandThread implements Runnable{
	private Scanner scanner = new Scanner(System.in);
	private volatile boolean done = false;
	private int m_whichThread = -1;
	private Client client = new Client();
	private Controller controller = new Controller();
	private ChunkServer chnkServ = new ChunkServer();
	
	public void setDone()
	{
		done = true;
	}
	
	public CommandThread(int whichThread)
	{
		m_whichThread = whichThread;
	}
	
	@Override
	public void run() {
		while(!done){
			if(m_whichThread == 0)
			{
				String command = scanner.nextLine();
				System.out.println("Command " + command);
				String[] token = command.split(" ");
				// For Client
				if(token[0].equals("store"))
				{
					String fileName = token[1];
					client.storeFile(fileName);
				}
				else if(token[0].equals("read"))
				{
					String fileName = token[1];
					int numSplits = Integer.parseInt(token[2]);
					client.readFile(fileName, numSplits);
				}
			}
			else if(m_whichThread == 1)
			{
				String command = scanner.nextLine();
				System.out.println("Command " + command);
				String[] token = command.split(" ");
				// For Controller
				if(token[0].equals("show-chunkServers"))
				{
					controller.printChunkServers();
				}
				else if(token[0].equals("show-fileChunkServerInfo"))
				{
					controller.printFileChunkServerInfo();;
				}
				else if(token[0].equals("show-fileMetaDataInfo"))
				{
					controller.printFileMetaDataInfo();
				}
			}
			else if(m_whichThread == 2)
			{
				String command = scanner.nextLine();
				System.out.println("Command " + command);
				String[] token = command.split(" ");
				//For Chunk Servers
				if(token[0].equals("show-fileInfo"))
				{
					chnkServ.printChunkInfo();
				}
			}
		}
	}
}
