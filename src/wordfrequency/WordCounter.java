package wordfrequency;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import crane.Bolt;

public class WordCounter extends Bolt implements Runnable{
	static volatile ConcurrentMap<String, AtomicLong> wordFreq = new ConcurrentHashMap<String, AtomicLong>();
	static AtomicLong wordcount;
	public WordCounter(int incomingTuplePort, String destinationIP,
			int destinationport) {
		super(incomingTuplePort, destinationIP, destinationport);
		wordcount=new AtomicLong(0L);
	}

	public static void main(String[] args) throws InterruptedException {
		int incomingTuplePort,destinationPort;
		String destinationIP;
		incomingTuplePort = Integer.parseInt(args[1]);
		destinationIP = args[2];
		destinationPort = Integer.parseInt(args[3]);
		WordCounter tokenizer = new WordCounter(incomingTuplePort,destinationIP,destinationPort);
		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(2);
		executor.execute(tokenizer);
		Thread.sleep(1200000);
		executor.shutdown();
	}
//	@Override
//	protected List<String> transformTuples(String inputstring)
//	{
//		List<String> result = new ArrayList<String>();
//		result.add(inputstring);
//		return result;
//	}
	
	@Override
	protected void processTuples()
	{
		String line=null;
		try {
			while((line = incomingTupleQueue.take())!=null)
			{
				String[] parsedLine = line.split("___");
				String extractedLine = parsedLine[0];
				wordFreq.putIfAbsent(extractedLine, new AtomicLong(0));
				wordFreq.get(extractedLine).incrementAndGet();
				wordcount.getAndIncrement();
				System.out.println("Bolt: Processed "+parsedLine[1]);
			}
		} catch (InterruptedException e) {
			System.out.println("ERROR: Bolt: Unable to take from incoming tuple	queue.");
			System.exit(0);
		}
	} 
	protected void write_result()
	{
		try {
			PrintWriter toFile = new PrintWriter(new FileOutputStream("output.log"));
			List<String> sortedKeys=new ArrayList<String>(wordFreq.keySet());
			Collections.sort(sortedKeys);
			String fileline;
			for(String word:sortedKeys)
			{
				fileline = word+","+wordFreq.get(word);
				toFile.println(fileline);
			}
			toFile.close();
		} catch (FileNotFoundException e) {
			System.out.println("Error: Bolt: Output file write failed.");
			System.exit(0);
		}

	}

	@Override
	protected void sendTuples()
	{
		while(true)
		{ 	
			try {
				Thread.sleep(10000);
				write_result();
			}	
			catch (InterruptedException e) {
				e.printStackTrace();
			}

		}
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
		Thread[] thread_processor = new Thread[3];
		for (int i=0;i<3;i++)
		{
			thread_processor[i] = new Thread(
					new Runnable() {
						@Override
						public void run()
						{
							processTuples();
						}
					}
					);
			thread_processor[i].start();
		}

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
	}


}
