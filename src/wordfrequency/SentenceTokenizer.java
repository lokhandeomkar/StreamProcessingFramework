package wordfrequency;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.regex.Pattern;

import crane.Bolt;

public class SentenceTokenizer extends Bolt implements Runnable{

	public SentenceTokenizer(int incomingTuplePort, String destinationIP,
			int destinationport) {
		super(incomingTuplePort, destinationIP, destinationport);
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) throws InterruptedException {
		int incomingTuplePort,destinationPort;
		String destinationIP;
		incomingTuplePort = Integer.parseInt(args[1]);
		destinationIP = args[2];
		destinationPort = Integer.parseInt(args[3]);
		SentenceTokenizer tokenizer = new SentenceTokenizer(incomingTuplePort,destinationIP,destinationPort);
		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(2);
		executor.execute(tokenizer);
		//		while(true);
		Thread.sleep(1200000);
		executor.shutdown();
	}
	
	//user defined transformation function
	@Override
	protected List<String> transformTuples(String inputstring)
	{
		
		String tokens[] = inputstring.replaceAll("[^a-zA-Z ]", "").toLowerCase().split("\\s+");
		List<String> result = new ArrayList<String>();
		for (String tuple: tokens)
		{
			result.add(tuple);
		}
		return result;
	}
	
//	protected void processTuples()
//	{
//		String line=null;
//		String[] tokens;
////		Pattern nonAlphaNumeric = Pattern.compile("[^a-zA-Z ]");
//
//		try {
//			long genTupleID=0;
//			long pred_id;
//			while((line = incomingTupleQueue.take())!=null)
//			{	
////				tokens = line.split("__");
////				line = tokens[0];
////				pred_id = Long.parseLong(tokens[1]);
//				
//				tokens=nonAlphaNumeric.matcher(line).replaceAll("").toLowerCase().split("\\s+");
//				for(String token:tokens)
//				{	
//					outgoingTupleQueue.add(token);
//					genTupleID++;
//					System.out.println("Bolt: Processed "+genTupleID+": "+token);
//				}
//			}
//		} 
//		catch (InterruptedException e) {
//			System.out.println("Bolt: Unable to take from incomingTupleQueue");
//		}
//	}
	
	
	
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
