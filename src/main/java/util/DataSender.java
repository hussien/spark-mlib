package util;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;

import javax.swing.JOptionPane;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;



public class DataSender {

	public static void socket_traning(int port) throws UnknownHostException, IOException{
		Socket socket = new Socket("localhost", port); // Create and connect the socket
		DataOutputStream dOut = new DataOutputStream(socket.getOutputStream());
		dOut.writeUTF("12,3213,345\n");
		dOut.writeUTF("13,3243,345\n");
		dOut.writeUTF("4,3223,34\n");
		dOut.writeUTF("15,33,534\n");
		dOut.writeUTF("13,32,112\n");
		
		dOut.flush(); // Send off the data
		dOut.close();

	}
	
	public static void socket_test(int port) throws UnknownHostException, IOException{
		Socket socket = new Socket("localhost", port); // Create and connect the socket
		DataOutputStream dOut = new DataOutputStream(socket.getOutputStream());

		dOut.writeUTF("1,4,3223,34\n");
		dOut.writeUTF("0,17,12,534\n");
		dOut.writeUTF("1,13,44,112\n");
		
		dOut.flush(); // Send off the data
		dOut.close();

	}
	
	
	public static void main(String args[]) throws UnknownHostException, IOException, ParseException{

		Options options = new Options();
		options.addOption("traning", true, "port for taining data");
		options.addOption("test", true, "port for test data");
		CommandLineParser parser = new BasicParser();
		CommandLine cmd = parser.parse( options, args);
		
		

		if(cmd.hasOption("traning")) {
			String port = cmd.getOptionValue("traning");
			socket_traning(Integer.parseInt(port));
		   
		}
		
		if(cmd.hasOption("test")) {
			String port = cmd.getOptionValue("test");
			socket_test(Integer.parseInt(port));
			   
		}
		
		
		




	}
}
