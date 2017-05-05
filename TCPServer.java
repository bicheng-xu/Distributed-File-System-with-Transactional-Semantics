import java.net.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.*;
import java.util.HashMap;

public class TCPServer {

	public static void main(String[] args) {

		  ServerSocket ss = null;
	      InetAddress addr;
	      int port = 0;
	      String ipAddr = "127.0.0.1"; 
	      String path = "";
	      
	      try {  
	         //Pass Parameters 
	    	 if (args.length != 3)
	    	 {
	    		 System.out.println("3 arguments are required to run server: ipAddress, port number and directory");
	    		 System.exit(0);
	    	 }
	    	 else
	    	 {
	    		 ipAddr = args[0];
	    		 try
					{
						port = (new Integer(args[1])).intValue();
					}
					catch(NumberFormatException nfe)
					{
						System.out.println("Invalid port: " + args[1]);
						System.exit(0);
					}
					if(port < 0 || port > 65536)
					{
						System.out.println("Invalid port: " + args[1]);
						System.exit(0);
					}
					path = args[2];
	    	 }
	         
	    	 //Create a server socket for listening
	    	 addr = InetAddress.getByName(ipAddr);  
	         ss = new ServerSocket(port, 20, addr);
	         System.out.println("Waiting for client on port " + 
	                     ss.getLocalPort() + "...");
	         
	         
	         TxnTable.getInstance().initServerLog(path);
	         
	         Runtime.getRuntime().addShutdownHook(new Thread()
	         {
	        	public void run()
	        	{
	        		// Gracefully shut down server
	        		TxnTable.getInstance().cleanUp();
	        	}
	         });
	         
	         //Accepting connection requests
	         while (true){
		         //listen and accept connection, return a client socket object	        	 
	      		Socket server = ss.accept(); 
	      		//Multithreading start
	      		RequestHandler rh = new RequestHandler(server, path);
	      		rh.start();
	         	System.out.println("Just connected to " + server.getRemoteSocketAddress());
	         }

	      } catch(IOException e) {
	         e.printStackTrace();
	      } 
	}
}
