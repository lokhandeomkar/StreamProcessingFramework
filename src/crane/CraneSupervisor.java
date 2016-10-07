package crane;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class CraneSupervisor implements Runnable
{

	protected static Queue<Integer> avlblPortList = new LinkedList<Integer>();
	protected static Queue<Integer> usedPortList = new LinkedList<Integer>();
	protected String jarFileName;
	protected String workerClassName;
	protected int workerSupervisorPort;
	protected String destinationIP;
	protected String destinationPort;
	protected String incomingTuplePort;
	
	public CraneSupervisor(String jarfilename, String worker_classname,int workerSupervisorPort, String destinationIP,
			String destinationPort, String incomingTuplePort) 
	{
		this.jarFileName = jarfilename;
		this.workerClassName=worker_classname;
		this.workerSupervisorPort = workerSupervisorPort;
		this.destinationIP = destinationIP;
		this.destinationPort= destinationPort;
		this.incomingTuplePort = incomingTuplePort;
	}
	public static void init_workerports(int supervisorNimbusPort)
	{
		int basePort = supervisorNimbusPort;
		avlblPortList.add(1+basePort);
		avlblPortList.add(2+basePort);
		avlblPortList.add(3+basePort);
		avlblPortList.add(4+basePort);
		avlblPortList.add(5+basePort);
	}
	public static String recv_bytemessage(BufferedInputStream fromSource) throws IOException
	{
		String incomingMessage = "";
		int msgLength = -1;
		int count=-1;
		byte[] msgSize = new byte[1];
		count = fromSource.read(msgSize,0,1);
		msgLength=(int) msgSize[0];
		byte[] incomingMsgByte = new byte[msgLength];
		count = fromSource.read(incomingMsgByte,0,msgLength);
		incomingMessage = new String(incomingMsgByte,0,msgLength);
		return incomingMessage;
	}
	public static void recv_byteFile(BufferedInputStream fromSource, String localfilename) throws IOException
	{
		int count=0;
		long totalcount=0;
		byte[] bfilesize = new byte[8];

		count = fromSource.read(bfilesize,0,8);
		ByteBuffer bb = ByteBuffer.wrap(bfilesize);
		int filesize = (int) bb.getLong();
		System.out.println("File size bytes: "+filesize);
		byte[] filechunk = new byte[8192];
		count = 0;
		BufferedOutputStream fileOutputStream = new BufferedOutputStream(new FileOutputStream(localfilename));
		int bytesread = 0;
		while(bytesread < filesize)
		{	
			int remainingbytes = filesize-bytesread;
			if (remainingbytes > 8192)
			{
				remainingbytes=8192;
			}
			int chunk = fromSource.read(filechunk,0,remainingbytes);
			fileOutputStream.write(filechunk,0,remainingbytes);
			bytesread += chunk;
		}
		System.out.println("Read bytes: "+bytesread);
		fileOutputStream.close();

	}
	public static void send_bytemessage(BufferedOutputStream todestination,String msg) throws IOException
	{
		byte[] msgbyte = msg.getBytes();
		byte outgoingmsgsize = (byte) msgbyte.length;

		//send the size of the message
		todestination.write(outgoingmsgsize);
		todestination.flush();

		//now send the byte array
		todestination.write(msgbyte,0,msgbyte.length);
		todestination.flush();
	}
	public static void main(String args[]) throws InterruptedException, IOException
	{
		System.out.println("Crane supervisor initiated.\nWorking Directory = " +System.getProperty("user.dir"));
		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(5);

		//setup a connection to listen from Nimbus
		int supervisornimbusport=Integer.parseInt(args[0]);
		ServerSocket nimbusserver = new ServerSocket(supervisornimbusport);
		Socket nimbussocket = nimbusserver.accept();
		System.out.println("Received a connection from Nimbus.");

		//nimbus will now send the jobdescription, read the incoming job
		BufferedInputStream fromNimbus = new BufferedInputStream(nimbussocket.getInputStream());

		//read the incoming message fromNimbus
		String incomingmessage;
		incomingmessage = recv_bytemessage(fromNimbus);
		System.out.println(incomingmessage);

		String[] commandToken = incomingmessage.split("\t");
		String workerType = commandToken[0];
		String worker_classname = commandToken[1];
		String incomingTuplePort = commandToken[3].split(":")[1];
		String destinationIP = commandToken[4].split(":")[0];
		String destinationPort = commandToken[4].split(":")[1];
		String spoutfilename="spoutfile.txt";
		String jarfilename = "recvd_crane"+supervisornimbusport+".jar";

		System.out.println("Receiving jar file from Nimbus");
		recv_byteFile(fromNimbus, jarfilename);

		if(workerType.equals("spout"))
		{
			System.out.println("Receiving spout file");
			recv_byteFile(fromNimbus, spoutfilename);
		}

		//job received. Start the job using the jar file and the appropriate startup commands

		// define the ports that can be used by the workers to communicate with the supervisor
		init_workerports(supervisornimbusport);

		// start the worker
		List<PrintWriter> toworkersockets = new ArrayList<PrintWriter>();
		List<BufferedReader> fromworkersockets = new ArrayList<BufferedReader>();
		CraneSupervisor handler;
		Socket workersocket;
		PrintWriter toworker;
		BufferedReader fromworker;

		// get the port for the server on the worker
		int workerSupervisorPort = avlblPortList.poll();
		// add it to the list of usedports
		usedPortList.add(workerSupervisorPort);

		//start a new thread for each worker
		handler = new CraneSupervisor(jarfilename,worker_classname, workerSupervisorPort,destinationIP,destinationPort, incomingTuplePort);		
		executor.execute(handler);

		//wait 1 second so that the worker can setup the server
		Thread.sleep(1000000);

//		//connect to the worker and initialize it		
//		workersocket = new Socket("localhost",workerSupervisorPort);
//		toworker = new PrintWriter(workersocket.getOutputStream(), true);
//		fromworker = new BufferedReader(new InputStreamReader(workersocket.getInputStream()));
//		toworkersockets.add(toworker);
//		fromworkersockets.add(fromworker);
//
//		//initializing command depends on what kind of worker it is
//		String initCommand=null;
//		if (workerType.equals("bolt"))
//		{		
//			initCommand = incomingTuplePort+"\t"+destinationIP+"\t"+destinationPort;
//		}
//		else if (workerType.equals("spout"))
//		{
//			initCommand = spoutfilename+"\t"+destinationIP+"\t"+destinationPort;
//		}
//
//		//send this command to the worker
//		toworker.println(initCommand);

//		boolean tester=true;
//		while(tester)
//		{
//
//		}
		executor.shutdown();
	}
	@Override
	public void run() {
		// TODO Auto-generated method stub
		try {
			Process pworker = new ProcessBuilder().inheritIO().command("java","-cp",jarFileName,workerClassName,Integer.toString(workerSupervisorPort),incomingTuplePort,destinationIP,destinationPort).start();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 


	}



}
