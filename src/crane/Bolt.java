package crane;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;


public class Bolt{
	protected int incomingTuplePort;
	protected String destinationIP;
	protected int destinationport;
	protected long timeout;
	protected volatile LinkedBlockingQueue<String> incomingTupleQueue = new LinkedBlockingQueue<String>(Integer.MAX_VALUE);
	protected volatile LinkedBlockingQueue<String> outgoingTupleQueue = new LinkedBlockingQueue<String>(Integer.MAX_VALUE);
	public Bolt(int incomingTuplePort, String destinationIP,int destinationport) {
		this.incomingTuplePort=incomingTuplePort;
		this.destinationIP = destinationIP;
		this.destinationport = destinationport;
		timeout=10;
	}

	protected void readTuples()
	{
		ServerSocket incomingTupleServer;
		try {
			incomingTupleServer = new ServerSocket(incomingTuplePort);
			Socket predecessor = incomingTupleServer.accept();
			System.out.println("Received connection");
			BufferedReader incomingStreamReader = new BufferedReader(new InputStreamReader(predecessor.getInputStream()));
			String fileLine;
			long count =0;

			while((fileLine = incomingStreamReader.readLine())!=null)
			{
				incomingTupleQueue.put(fileLine);
				count++;
				System.out.println("Bolt: Read\t"+count);
			}
		}
		catch (IOException e) {
			System.out.println("ERROR: Bolt: Unable to open server on: "+incomingTuplePort);
			System.exit(0);
		} catch (InterruptedException e) {
			System.out.println("ERROR: Bolt: Unable to add to incomingTupleQueue");
			System.exit(0);
		}

	}
	
	protected List<String> transformTuples(String inputstring)
	{
		List<String> result = new ArrayList<String>();
		return result;
	}
	
	protected void processTuples()
	{
		String line=null;
		try {
			while((line = incomingTupleQueue.take())!=null)
			{	
				long genTupleID=0;
				String[] parsedLine = line.split("___");
				String predIDS = parsedLine[1];
				
				List<String> transformedTuples = transformTuples(parsedLine[0]);
				
				synchronized (outgoingTupleQueue)
				{
					for (String tuple:transformedTuples)
					{
						genTupleID++;
						outgoingTupleQueue.add(tuple+"___"+predIDS+"."+genTupleID);
						System.out.println("Bolt: Processed "+predIDS+"."+genTupleID);
					}
				}
			}
		} 
		catch (InterruptedException e) {
			System.out.println("Bolt: Unable to take from incomingTupleQueue");
		}
	}
	protected void sendTuples()
	{
		try {
			Socket destination = new Socket(destinationIP,destinationport);
			PrintWriter todest = new PrintWriter(destination.getOutputStream(),true);	
			String line=null;
			int count=0;
			while((line = outgoingTupleQueue.take())!=null)
			{
				todest.println(line);
				count++;
				System.out.println("Bolt: Sent\t"+count);
			}
			todest.flush();
		}
		catch (IOException e) {
			System.out.println("ERROR: Bolt: Unable to send data to "+destinationIP+":"+destinationport);
			System.exit(0);
		} 
		catch (InterruptedException e) {
			System.out.println("ERROR: Bolt: Unable to take from outgoingTupleQueue");
			System.exit(0);
		}
	}
}
