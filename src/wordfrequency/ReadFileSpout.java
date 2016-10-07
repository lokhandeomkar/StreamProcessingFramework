package wordfrequency;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import crane.Spout;

public class ReadFileSpout extends Spout implements Runnable{

	public ReadFileSpout(String filetoread, String destinationIP,
			int destinationport) 
	{
		super(filetoread, destinationIP, destinationport);
	}

	public static void main(String[] args) throws InterruptedException {
		int incomingTuplePort,destinationPort;
		String destinationIP;
		incomingTuplePort = Integer.parseInt(args[1]);
		destinationIP = args[2];
		destinationPort = Integer.parseInt(args[3]);
		ReadFileSpout reader = new ReadFileSpout("spoutfile.txt",destinationIP,destinationPort);
		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(2);
		executor.execute(reader);
		Thread.sleep(1200000);
		executor.shutdown();
	}

	@Override
	public void run() {
		Thread thread_receiver = new Thread(
				new Runnable() {
					@Override
					public void run()
					{
						readTuples();
					}
				}
				);
		thread_receiver.start();
		Thread thread_sender = new Thread(
				new Runnable() {
					@Override
					public void run()
					{
						sendTuples();
					}
				}
				);
		thread_sender.start();
		while(true);
	}

}
