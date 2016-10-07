package crane;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Spout{
	protected String filetoread;
	protected String destinationIP;
	protected int destinationport;
	protected volatile LinkedBlockingQueue<String> tupleQueue = new LinkedBlockingQueue<String>(Integer.MAX_VALUE);
	public Spout(String filetoread, String destinationIP,int destinationport) {
		this.filetoread=filetoread;
		this.destinationIP = destinationIP;
		this.destinationport = destinationport;
	}

	protected void readTuples()
	{
		try {
			BufferedReader fileReader = new BufferedReader(new InputStreamReader(new FileInputStream(filetoread)));
			String fileLine;
			while((fileLine = fileReader.readLine())!=null)
			{
				tupleQueue.put(fileLine);
			}
			System.out.println("DONE: Spout: Finished Reading file");
			fileReader.close();
		} catch (FileNotFoundException e) {
			System.out.println("ERROR: Spout : Unable to open file: "+filetoread);
			System.exit(0);
		} catch (IOException e) {
			System.out.println("ERROR: Spout : Unable to read file: "+filetoread);
			System.exit(0);
		} catch (InterruptedException e) {
			System.out.println("ERROR: Spout : Unable to add to tupleQueue");
			System.exit(0);
		}
	}
	protected void sendTuples()
	{

		try {
			Socket destination = new Socket(destinationIP,destinationport);
			PrintWriter todest = new PrintWriter(destination.getOutputStream(),true);	
			long counter=0;
			String line;
			System.out.println("Sending thread active.");
			while((line = tupleQueue.take())!=null)
			{
				counter++;
				todest.println(line+"___"+counter);
				System.out.println("Spout: Sent "+counter);
			}
		}
		catch (IOException e) {
			System.out.println("ERROR: Spout : Unable to send data to "+destinationIP+":"+destinationport);
			System.exit(0);
		} catch (InterruptedException e) {
			System.out.println("ERROR: Spout : Unable to take from tupleQueue");
			System.exit(0);
		} 

	}
}
