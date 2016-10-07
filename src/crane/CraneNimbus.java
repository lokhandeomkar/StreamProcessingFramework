package crane;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CraneNimbus {
	
	protected static List<String> assignjobs(BufferedReader jobfilereader,List<String> memList,int supervisorport) throws IOException
	{
		List<String> assignedjobs = new ArrayList<String>();
		String jobfileline, assignedworker,successorworker,jobentry;
		int incomingport = 9000;
		int workercounter=0;
		while((jobfileline = jobfilereader.readLine())!=null)
		{
			assignedworker = memList.get(workercounter%memList.size());
			successorworker = memList.get((workercounter+1)%memList.size());
			jobentry = jobfileline+"\t"+supervisorport+"\t"+assignedworker+":"+incomingport+"\t"+successorworker+":"+(incomingport+1);
			assignedjobs.add(jobentry);
			incomingport++;
			//disable for vm testing
			supervisorport+=100;
		}
		jobfilereader.close();
		return assignedjobs;
	}

	
	public static Map<String,Socket> init_crane(String spoutfilename,String jarfilename,List<String> assignedjobs) throws UnknownHostException, IOException, InterruptedException
	{
		Map<String,Socket> workersockets = new HashMap<String,Socket>();
		String jobtokens[];
		Socket workersocket;
		BufferedInputStream fromworker;
		BufferedOutputStream toworker;

		// the assignedlist is formatted as
		// spout spoutclass supervisorport inputip:inputport outputip:outputport
		// jobmessage: spout spoutclass supervisorport inputip:inputport outputip:outputport
		File jarfile = new File(jarfilename);
		File spoutfile=null;
		if(!spoutfilename.equals("NULL.txt")) spoutfile = new File(spoutfilename);
		for(String jobentry:assignedjobs)
		{
			jobtokens = jobentry.split("\t");
			String inputokens[] = jobtokens[3].split(":");
			String destination = inputokens[0];
			System.out.print("Setting up "+jobtokens[1]+" on "+jobtokens[3]+": Sent");
			workersocket = new Socket(destination,Integer.parseInt(jobtokens[2]));
			toworker = new BufferedOutputStream(workersocket.getOutputStream());
			fromworker = new BufferedInputStream(workersocket.getInputStream()); 
			workersockets.put(jobentry,workersocket);
			send_bytemessage(toworker, jobentry);
			if (jarfile!=null)
			{
				send_byteFile(toworker, jarfile);
			}
			toworker.flush();
			
			if (jobtokens[0].equals("spout"))
			{
				send_byteFile(toworker, spoutfile);
			}
			System.out.print(".\n");
			Thread.sleep(1000);
		}
		return workersockets;
	}
	protected static void send_bytemessage(BufferedOutputStream todestination,String msg) throws IOException
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
	protected static void send_byteFile(BufferedOutputStream todestination,File file) throws IOException
	{
		Long filesize =  file.length();
		
		byte[] bfilesize = ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(filesize).array();
		//send the size of the file
		todestination.write(bfilesize);
		todestination.flush();
		
		byte[] filechunk = new byte[8192];
		int count = -1;
		BufferedInputStream fromFile = new BufferedInputStream(new FileInputStream(file));
		
		while((count = fromFile.read(filechunk))>=0)
		{
			todestination.write(filechunk,0,count);
		}
		System.out.print(" "+file.getName()+"("+filesize+" bytes)");
		todestination.flush();
		fromFile.close();
	}
	protected static String recv_bytemessage(BufferedInputStream fromSource) throws IOException
	{
		String incomingmessage = "";
		int totalbyte = 0;
		int msglength = -1;
		int count=-1;
		byte[] msgsize = new byte[1];
		count = fromSource.read(msgsize,0,1);
		msglength=(int) msgsize[0];

		byte[] incomingmsgbyte = new byte[msglength];
		count = fromSource.read(incomingmsgbyte,0,msglength);
		incomingmessage = new String(incomingmsgbyte,0,msglength);
		return incomingmessage;
	}

	public static void main(String[] args) throws IOException, InterruptedException {

		String jarfilename=null;
		String spoutfilename = null;
		List<String> memList = new ArrayList<String>();
		List<String> assignedjobs = new ArrayList<String>();
		int supervisorport=8000;

		BufferedReader inputStream = new BufferedReader(new InputStreamReader(System.in));		
		System.out.print("Enter the jobfile: ");
		String jobfilename = inputStream.readLine();

		//get memblist
		memList.add("localhost");

		//open the job file
		BufferedReader jobFileReader = new BufferedReader(new FileReader(jobfilename));
		String jobfileline = jobFileReader.readLine();

		// the first line will specify the jarfile and spoutfile
		String[] filenames = jobfileline.split("\t");
		if (filenames.length == 1)
		{
			jarfilename = filenames[0];
			spoutfilename = "NULL.txt";
		}
		else
		{
			jarfilename = filenames[0];
			spoutfilename=filenames[1];
		}
		System.out.println("Spout:"+spoutfilename+", Jar:"+jarfilename);
		//		if (spoutfilename!=null) spoutfile = new File(spoutfilename);		

		//get the membership list and assign jobs to different machines
		assignedjobs = assignjobs(jobFileReader, memList, supervisorport);
		System.out.println("Assigned jobs: "+assignedjobs.toString());
		
		init_crane(spoutfilename,jarfilename,assignedjobs);
	}
}
