package util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;
import java.util.HashSet;

import org.apache.spark.sql.catalyst.expressions.In;

public class DataServer {


	private static HashSet<PrintWriter> writers = new HashSet<PrintWriter>();


	public static void main(String args[]) throws IOException{

		
		int clientNumber = 0;
		int port = Integer.parseInt(args[0]);
		ServerSocket listener = new ServerSocket(port);
		System.out.println("The socket server on port "+port+" is running.");
		try {

			while (true) {
				new Handler(listener.accept(), clientNumber++).start();
			}
		}
		finally {
			listener.close();

		}

	}

	private static class Handler extends Thread {
		private Socket socket;
		private int clientNumber;

		public Handler(Socket socket, int clientNumber) {
			this.socket = socket;
			this.clientNumber = clientNumber;
			log("New connection with client# " + clientNumber + " at " + socket);
		}

		/**
		 * Services this thread's client by first sending the
		 * client a welcome message then repeatedly reading strings
		 * and sending back the capitalized version of the string.
		 */
		public void run() {
			try {

				// Decorate the streams so we can send characters
				// and not just bytes.  Ensure output is flushed
				// after every newline.
				BufferedReader in = new BufferedReader(
						new InputStreamReader(socket.getInputStream()));
				PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
				writers.add(out);

				// Send a welcome message to the client.
				//out.println("Hello, you are client #" + clientNumber + ".");
				//out.println("Enter a line with only a period to quit\n");

				// Get messages from the client, line by line; return them
				// capitalized
				while (true) {
					String input = in.readLine();
					if (input == null || input.equals(".")) {
						break;
					}
					for (PrintWriter writer : writers) {
						writer.println(input);
					}

				}
			} catch (IOException e) {
				log("Error handling client# " + clientNumber + ": " + e);
			} finally {
				try {
					socket.close();
				} catch (IOException e) {
					log("Couldn't close a socket, what's going on?");
				}
				log("Connection with client# " + clientNumber + " closed");
			}
		}

		/**
		 * Logs a simple message.  In this case we just write the
		 * message to the server applications standard output.
		 */
		private void log(String message) {
			System.out.println(message);
		}
	}


}
