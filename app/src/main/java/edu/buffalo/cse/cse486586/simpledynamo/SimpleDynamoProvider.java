package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.Iterator;
import java.util.List;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {
	String myPort;
	static final int SERVER_PORT = 10000;
	private List<String> nodesList = new ArrayList<String>();
	Context context;
	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	String successorPort1="";
	String successorPort2="";
	String prepredecessorport="";
	String predecessorport="";

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub

		//now work on delete
		//now for failure, when delete arrives,
		//send same key to all in the ring to delete so that when I fail and come back I dont get that data anyhow


		String keyToDelete = selection;
		Log.v(TAG, "key to be deleted is : " + keyToDelete);
		context = getContext();
		try {
			context.deleteFile(keyToDelete);
		} catch (Exception e) {
			Log.e(TAG, "delete: Error occurred in delete");
		}

		for (int i=0;i<nodesList.size();i++)
		{
			if(!(nodesList.get(i).equals(myPort)))
			{
				//Create socket connection to Server and ask him to delete
				StringBuilder dataToSend = new StringBuilder();
				dataToSend.append("Delete").append("&").append(keyToDelete);
				String concatenatedMessage=dataToSend.toString();


				try {
					Socket specialsocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(nodesList.get(i)) * 2);
					DataOutputStream specialdataOutputStream = new DataOutputStream(specialsocket.getOutputStream());
					specialdataOutputStream.writeUTF(concatenatedMessage);
					Log.v(TAG, " Special Message sent to " + successorPort1 + " and message is : " + concatenatedMessage);
					specialsocket.close();
				} catch (UnknownHostException e) {
					Log.e(TAG, "doInBackground: Unknown host exception occurred");
				} catch (IOException e) {
					Log.e(TAG, "doInBackground: IO exception occurred 2");
					e.printStackTrace();

				}
			}
		}


		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
		Log.d(TAG, "insert: Insert query received from grader");


		String keytoInsert=values.get("key").toString();
		String valuetoInsert=values.get("value").toString();

		String destinationPort = null;
		//Now check where the key should reside
		try {
			boolean foundDestinationFlag=false;
			for (int i=0;i<nodesList.size();i++)
			{
				String porttoCheck=nodesList.get(i);
				int comparisonVal=genHash(keytoInsert).compareTo(genHash(porttoCheck));
				if(comparisonVal<=0)
				{
					destinationPort=porttoCheck;
					foundDestinationFlag=true;
					break;
				}
			}
			if (foundDestinationFlag==false) {
				destinationPort = nodesList.get(0);
			}
			Log.v(TAG, "destination port for key : " + keytoInsert + " is :" + destinationPort);
			Log.v(TAG,"my port is "+myPort);

		} catch (NoSuchAlgorithmException e) {
			Log.e(TAG, "doInBackground: Error exception occurred in genhash comparison");
		}
		Log.v(TAG,"my port is "+myPort);
		if(destinationPort.equals(myPort))
		{
			Log.v(TAG, "insert: inside dest port equals myport ");
			String file_name=values.get("key").toString();
			String data = values.get("value").toString();
			context=getContext();
			try {
				FileOutputStream fos = context.openFileOutput(file_name, Context.MODE_PRIVATE);
				fos.write(data.getBytes());
				fos.close();

			}
			catch(IOException e)
			{
				Log.v("Failed to insert",values.toString());
			}
			//now find out my successors for replication
			//String successorPort1="";
			//String successorPort2="";
			int myIndex=0;
			balanceChord();
			for(int i=0;i<nodesList.size();i++)
			{
			//FIRST FIND OUT MY INDEX
				if(nodesList.get(i).equals(myPort)){
					myIndex=i;
					break;
				}
			}
			if(myIndex==0 || myIndex==1 || myIndex==2 )
			{
				successorPort1=nodesList.get(myIndex+1);
				successorPort2=nodesList.get(myIndex+2);
			}
			else if(myIndex==3)
			{
				successorPort1=nodesList.get(4);
				successorPort2=nodesList.get(0);
			}
			else if(myIndex==4)
			{
				successorPort1=nodesList.get(0);
				successorPort2=nodesList.get(1);
			}
			Log.v(TAG,"My port is "+myPort);
			Log.v(TAG, "My successor is " + successorPort1);
			Log.v(TAG, "My successor 2 is " + successorPort2);
			//send data for replication to my successors for inserting

			//create message to send
			StringBuilder dataToSend = new StringBuilder();
			dataToSend.append("InsertReplicate").append("&").append(keytoInsert).append("&").append(valuetoInsert);
			String concatenatedMessage=dataToSend.toString();


			try {
				Socket specialsocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successorPort1) * 2);
				DataOutputStream specialdataOutputStream = new DataOutputStream(specialsocket.getOutputStream());
				specialdataOutputStream.writeUTF(concatenatedMessage);
				Log.v(TAG, " Special Message sent to " + successorPort1 + " and message is : " + concatenatedMessage);
			} catch (UnknownHostException e) {
				Log.e(TAG, "doInBackground: Unknown host exception occurred");
			} catch (IOException e) {
				Log.e(TAG, "doInBackground: IO exception occurred 2");
				e.printStackTrace();

			}
			try{
				Socket specialsocket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successorPort2) * 2);
				DataOutputStream specialdataOutputStream1 = new DataOutputStream(specialsocket1.getOutputStream());
				specialdataOutputStream1.writeUTF(concatenatedMessage);
				Log.v(TAG, " Special Message sent to " + successorPort2 + " and message is : " + concatenatedMessage);


			} catch (UnknownHostException e) {
				Log.e(TAG, "doInBackground: Unknown host exception occurred");
			} catch (IOException e) {
				Log.e(TAG, "doInBackground: IO exception occurred 2");
				e.printStackTrace();

			}



		}
		else {
			//case where message to be inserted is not in my own directory but has to be sent to concerned server
			StringBuilder dataToSend = new StringBuilder();
			dataToSend.append("Insert").append("&").append(keytoInsert).append("&").append(valuetoInsert);
			String concatenatedMessage = dataToSend.toString();



			String newsuccessorPort1="";
			String newsuccessorPort2="";
			int newIndex=0;
			balanceChord();
			for(int i=0;i<nodesList.size();i++)
			{
				//FIRST FIND OUT MY INDEX
				if(nodesList.get(i).equals(destinationPort)){
					newIndex=i;
					break;
				}
			}
			if(newIndex==0 || newIndex==1 || newIndex==2 )
			{
				newsuccessorPort1=nodesList.get(newIndex+1);
				newsuccessorPort2=nodesList.get(newIndex+2);
			}
			else if(newIndex==3)
			{
				newsuccessorPort1=nodesList.get(4);
				newsuccessorPort2=nodesList.get(0);
			}
			else if(newIndex==4)
			{
				newsuccessorPort1=nodesList.get(0);
				newsuccessorPort2=nodesList.get(1);
			}
			Log.v(TAG,"New port is "+destinationPort);
			Log.v(TAG, "New successor is " + newsuccessorPort1);
			Log.v(TAG, "New successor 2 is " + newsuccessorPort2);





			try {
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(destinationPort) * 2);
				DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
				dataOutputStream.writeUTF(concatenatedMessage);
				Log.v(TAG, " Special Message sent to " + destinationPort + " and message is : " + concatenatedMessage);

			} catch (UnknownHostException e) {
				Log.e(TAG, "doInBackground: Unknown host exception occurred");
			} catch (IOException e) {
				Log.e(TAG, "doInBackground: IO exception occurred 2");
				e.printStackTrace();

			}


			StringBuilder newdataToSend = new StringBuilder();
			newdataToSend.append("InsertReplicate").append("&").append(keytoInsert).append("&").append(valuetoInsert);
			concatenatedMessage = newdataToSend.toString();
			if(newsuccessorPort1.equals(myPort))
			{
				String file_name=keytoInsert;
				String data=valuetoInsert;
				context=getContext();
				try {
					FileOutputStream fos = context.openFileOutput(file_name, Context.MODE_PRIVATE);
					fos.write(data.getBytes());
					fos.close();

				}
				catch(IOException e)
				{
					Log.v("Failed to insert",values.toString());
				}

				//when new successor port is my own so do local insert

			}
			else{
			try {
				Socket specialsocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(newsuccessorPort1) * 2);
				DataOutputStream specialdataOutputStream = new DataOutputStream(specialsocket.getOutputStream());
				specialdataOutputStream.writeUTF(concatenatedMessage);
				Log.v(TAG, " Special Message sent to " + newsuccessorPort1 + " and message is : " + concatenatedMessage);
			} catch (UnknownHostException e) {
				Log.e(TAG, "doInBackground: Unknown host exception occurred");
			} catch (IOException e) {
				Log.e(TAG, "doInBackground: IO exception occurred 2");
				e.printStackTrace();
			}

			}
			if(newsuccessorPort2.equals(myPort))
			{
				//when new successor port is my own so do local insert
				String file_name=keytoInsert;
				String data=valuetoInsert;
				context=getContext();
				try {
					FileOutputStream fos = context.openFileOutput(file_name, Context.MODE_PRIVATE);
					fos.write(data.getBytes());
					fos.close();

				}
				catch(IOException e)
				{
					Log.v("Failed to insert",values.toString());
				}

			}
			else {
				try {
					Socket specialsocket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(newsuccessorPort2) * 2);
					DataOutputStream specialdataOutputStream1 = new DataOutputStream(specialsocket1.getOutputStream());
					specialdataOutputStream1.writeUTF(concatenatedMessage);
					Log.v(TAG, " Special Message sent to " + newsuccessorPort2 + " and message is : " + concatenatedMessage);


				} catch (UnknownHostException e) {
					Log.e(TAG, "doInBackground: Unknown host exception occurred");
				} catch (IOException e) {
					Log.e(TAG, "doInBackground: IO exception occurred 2");
					e.printStackTrace();

				}
			}



		}

		return uri;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub



		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort=portStr;
		nodesList.add("5554");
		nodesList.add("5556");
		nodesList.add("5558");
		nodesList.add("5560");
		nodesList.add("5562");
		balanceChord();
		//find successor1 successror2 and preprepredecessor
		int myIndex=0;
		for(int i=0;i<nodesList.size();i++)
		{
			//FIRST FIND OUT MY INDEX
			if(nodesList.get(i).equals(myPort)){
				myIndex=i;
				break;
			}
		}
		if(myIndex==0 || myIndex==1 || myIndex==2 )
		{
			successorPort1=nodesList.get(myIndex+1);
			successorPort2=nodesList.get(myIndex+2);
		}
		else if(myIndex==3)
		{
			successorPort1=nodesList.get(4);
			successorPort2=nodesList.get(0);
		}
		else if(myIndex==4)
		{
			successorPort1=nodesList.get(0);
			successorPort2=nodesList.get(1);
		}
		prepredecessorport=nodesList.get((myIndex+3)%5);
		predecessorport=nodesList.get((myIndex+4)%5);
		Log.v(TAG, "My port is " + myPort);
		Log.v(TAG, "My successor is " + successorPort1);
		Log.v(TAG, "My successor 2 is " + successorPort2);
		Log.v(TAG, "My prepredecessor port is " + prepredecessorport);




		try {ServerSocket serverSocket = new ServerSocket(SERVER_PORT);//creating the server on 10000
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);//sending to accept connections on the serversocket
		} catch (IOException e) {
			Log.v(TAG, "Problem occurred while creating server socket");
			return false;}

		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,myPort,"Reborn" );


		//can I directly create server socket from on create
		//create message to be sent to successor and prepredecessor




		return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub


		MatrixCursor matrixCursor=new MatrixCursor(new String[]{"key","value"});

		String queryParameter=selection;
			if (queryParameter.equals("@")) {

				context = getContext();
				String fileList[] = context.fileList();
				for (int i = 0; i < fileList.length; i++) {
					String file_name = fileList[i];
					try {
						FileInputStream fis = context.openFileInput(file_name);
						InputStreamReader inputStreamReader = new InputStreamReader(fis);
						BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
						StringBuilder sb = new StringBuilder();
						String line;
						while ((line = bufferedReader.readLine()) != null) {
							sb.append(line);
						}
						Log.e("data read", sb.toString());
						String[] row = {file_name, sb.toString()};

						matrixCursor.addRow(row);

						fis.close();
						//return matrixCursor;
					} catch (IOException e) {
						Log.v("Failed to query", selection);
					}

				}
			}
			else if(queryParameter.equals("*")) {
				//handle * case here
				//split into various parts/
				//First get my own @ data, copy code from above and put it into a string
				//Then send this query to all other nodes one by one and ask them to send back their @ data as a string
				//get one giant string and put all data into matrix cursor
				//First part starts here

				context = getContext();
				String fileList[] = context.fileList();
				for (int i = 0; i < fileList.length; i++) {
					String file_name = fileList[i];
					try {
						FileInputStream fis = context.openFileInput(file_name);
						InputStreamReader inputStreamReader = new InputStreamReader(fis);
						BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
						StringBuilder sb = new StringBuilder();
						String line;
						while ((line = bufferedReader.readLine()) != null) {
							sb.append(line);
						}
						Log.e("data read", sb.toString());
						String[] row = {file_name, sb.toString()};

						matrixCursor.addRow(row);

						fis.close();
						//return matrixCursor;
					} catch (IOException e) {
						Log.v("Failed to query", selection);
					}

				}
				//now run a loop on the nodes list and handle each data returned separately
				for (int i = 0; i < nodesList.size(); i++) {
					if (!(nodesList.get(i).equals(myPort))) {
						//this is the case of all other ports

						StringBuilder dataToSend = new StringBuilder();
						dataToSend.append("QueryGlobal").append("&").append("Local");
						String concatenatedMessage = dataToSend.toString();
						String destinationPort = nodesList.get(i);
						try {
							Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(destinationPort) * 2);
							DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
							dataOutputStream.writeUTF(concatenatedMessage);
							Log.v(TAG, " Special Message sent to " + destinationPort + " and message is : " + concatenatedMessage);

							//wait here for reply//modify  here below
							DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());

							try {
								String messageFromServer = dataInputStream.readUTF();
								Log.v(TAG, "doInBackground: Client Task : message received is :" + messageFromServer);
								if (messageFromServer == null) {
									Log.v(TAG, "doInBackground: server sent nothing ");


								} else {
									Log.v(TAG, "server says that :" + messageFromServer);
									String[] tokenisedData = messageFromServer.split("&");
									if (tokenisedData[0].equals("QueryGlobalOutput")) {
										//correct data recieved

										for (int h=1;h<tokenisedData.length;h++)
										{
											String [] tokeniseLvl2=tokenisedData[h].split("-");
											String keyToQuery = tokeniseLvl2[0];
											String value = tokeniseLvl2[1];
											String[] row = {keyToQuery, value};
											matrixCursor.addRow(row);

										}
									}

								}
							} catch (IOException e) {
								Log.v(TAG, "IO Exception occurred in query response");
							}

						} catch (UnknownHostException e) {
							Log.e(TAG, "doInBackground: Unknown host exception occurred");
						} catch (IOException e) {
							Log.e(TAG, "doInBackground: IO exception occurred 2");
							e.printStackTrace();

						}

					}
				}
			}
		else{
				//case where single key is to be queried for
				String file_name=selection;
				context=getContext();
				//check whether I myself have that file or not
				String fileList[] = context.fileList();
				boolean hasLocal = false;
				for(int i=0;i<fileList.length;i++)
				{
					if(fileList[i].equals(file_name))
					{
						hasLocal=true;
						break;
					}
				}
		if(hasLocal==true) {
			//case where I have the key value pair
			try {
				FileInputStream fis = context.openFileInput(file_name);
				InputStreamReader inputStreamReader = new InputStreamReader(fis);
				BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
				StringBuilder sb = new StringBuilder();
				String line;
				while ((line = bufferedReader.readLine()) != null) {
					sb.append(line);
				}
				Log.e("data read", sb.toString());
				String[] row = {selection, sb.toString()};

				matrixCursor.addRow(row);

				fis.close();
			} catch (IOException e) {
				Log.v("Failed to query", selection);
			}
		}
				else
		{
			String keytoQuery=selection;
			String destinationPort=null;
			//case where I dont have the key value pair
			//calculate who should have this key value pair
			try {

				boolean foundDestinationFlag=false;
				for (int i=0;i<nodesList.size();i++)
				{
					String porttoCheck=nodesList.get(i);
					int comparisonVal=genHash(keytoQuery).compareTo(genHash(porttoCheck));
					if(comparisonVal<=0)
					{
						destinationPort=porttoCheck;
						foundDestinationFlag=true;
						break;
					}
				}
				if (foundDestinationFlag==false) {
					destinationPort = nodesList.get(0);
				}
				Log.v(TAG, "destination port for key : " + keytoQuery + " is :" + destinationPort);
				Log.v(TAG,"my port is "+myPort);

			} catch (NoSuchAlgorithmException e) {
				Log.e(TAG, "doInBackground: Error exception occurred in genhash comparison");
			}

			//now send Query message to concerned port for query
			StringBuilder dataToSend = new StringBuilder();
			dataToSend.append("Query").append("&").append(keytoQuery);
			String concatenatedMessage = dataToSend.toString();

			try {
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(destinationPort) * 2);
				DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
				dataOutputStream.writeUTF(concatenatedMessage);
				Log.v(TAG, " Special Message sent to " + destinationPort + " and message is : " + concatenatedMessage);

				//wait here for reply
				DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());

				try {
					String messageFromServer = dataInputStream.readUTF();
					Log.v(TAG, "doInBackground: Client Task : message received is :" + messageFromServer);
					if (messageFromServer == null) {
						Log.v(TAG, "doInBackground: server sent nothing ");


					} else {
						Log.v(TAG, "server says that :" + messageFromServer);
						String[] tokenisedData = messageFromServer.split("&");
						if(tokenisedData[0].equals("QueryOutput"))
						{
							//correct data recieved
							String keyToQuery=tokenisedData[1];
							String value=tokenisedData[2];
							String[] row = {keyToQuery, value};

							matrixCursor.addRow(row);

						}

					}
				}
				catch (IOException e)
				{
					Log.v(TAG,"IO Exception occurred in query response");
					//Flow comes here in case of Exception
					//handle the further proceedings//
					//Send same Query message to its successor
					//Find successor
					int indextoLocate=0;
					for(int i=0;i<nodesList.size();i++)
					{
						//FIRST FIND OUT MY INDEX
						if(nodesList.get(i).equals(destinationPort)){
							indextoLocate=i;
							break;
						}
					}
					String succInFailure=nodesList.get((indextoLocate+1)%5);
					try {
						Socket socketnew = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(succInFailure) * 2);
						DataOutputStream dataOutputStreamNew = new DataOutputStream(socketnew.getOutputStream());
						dataOutputStreamNew.writeUTF(concatenatedMessage);
						Log.v(TAG, " Special Message sent to " + succInFailure + " and message is : " + concatenatedMessage);

						//wait here for reply
						DataInputStream dataInputStreamnew = new DataInputStream(socketnew.getInputStream());

						try {
							String messageFromServer = dataInputStreamnew.readUTF();
							Log.v(TAG, "doInBackground: Client Task : message received is :" + messageFromServer);
							if (messageFromServer == null) {
								Log.v(TAG, "doInBackground: server sent nothing ");


							} else {
								Log.v(TAG, "server says that :" + messageFromServer);
								String[] tokenisedData = messageFromServer.split("&");
								if (tokenisedData[0].equals("QueryOutput")) {
									//correct data recieved
									String keyToQuery = tokenisedData[1];
									String value = tokenisedData[2];
									String[] row = {keyToQuery, value};

									matrixCursor.addRow(row);

								}

							}
						} catch (IOException e1) {
							Log.v(TAG,"IO Exception occurred in query response inside Exception");
						}
					}
					catch (IOException e2)
					{
						Log.v(TAG,"IO Exception occurred in query response inside Exception 2");
					}



			}
			} catch (IOException e) {
				Log.e(TAG, "doInBackground: IO exception occurred 2");
				e.printStackTrace();

			}


		}


			}


		return matrixCursor;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			Log.v(TAG,"Inside Doinbackground for Server task of port number : "+myPort);
			ServerSocket serverSocket=sockets[0];

			try {
				while (true) {
					Socket server = serverSocket.accept();
					DataInputStream dataInputStream = new DataInputStream(server.getInputStream());
					String messageFromClient = dataInputStream.readUTF();
					String messageToClient = null;

					if (messageFromClient == null) {
						Log.v(TAG, "No data received from client");
					} else {
						Log.v(TAG, "Message on : " + myPort + " received and message is : " + messageFromClient);
						String[] tokenisedData = messageFromClient.split("&");
						String operationToPerform = tokenisedData[0];
						if (operationToPerform.equals("InsertReplicate")) {
							Log.v(TAG, "Server task do in background InsertReplicate message received at port: " + myPort);
							String keytoInsert = tokenisedData[1];
							String valuetoInsert = tokenisedData[2];

							String file_name = keytoInsert;
							String data = valuetoInsert;
							context = getContext();
							try {
								FileOutputStream fos = context.openFileOutput(file_name, Context.MODE_PRIVATE);
								fos.write(data.getBytes());
								fos.close();

							} catch (IOException e) {
								Log.v("Failed to insert", keytoInsert);
							}


						} else if (operationToPerform.equals("Insert")) {
							String keytoInsert = tokenisedData[1];
							String valuetoInsert = tokenisedData[2];


							Log.v(TAG, "insert: inside dest port equals myport ");
							String file_name = keytoInsert;
							String data = valuetoInsert;
							context = getContext();
							try {
								FileOutputStream fos = context.openFileOutput(file_name, Context.MODE_PRIVATE);
								fos.write(data.getBytes());
								fos.close();

							} catch (IOException e) {
								Log.v("Failed to insert", keytoInsert);
							}
							//now find out my successors for replication
//							String successorPort1="";
//							String successorPort2="";
//							int myIndex=0;
//							balanceChord();
//							for(int i=0;i<nodesList.size();i++)
//							{
//								//FIRST FIND OUT MY INDEX
//								if(nodesList.get(i).equals(myPort)){
//									myIndex=i;
//									break;
//								}
//							}
//							if(myIndex==0 || myIndex==1 || myIndex==2 )
//							{
//								successorPort1=nodesList.get(myIndex+1);
//								successorPort2=nodesList.get(myIndex+2);
//							}
//							else if(myIndex==3)
//							{
//								successorPort1=nodesList.get(4);
//								successorPort2=nodesList.get(0);
//							}
//							else if(myIndex==4)
//							{
//								successorPort1=nodesList.get(0);
//								successorPort2=nodesList.get(1);
//							}
//							Log.v(TAG,"My port is "+myPort);
//							Log.v(TAG, "My successor is " + successorPort1);
//							Log.v(TAG, "My successor 2 is " + successorPort2);
//							//send data for replication to my successors for inserting
//
//							//create message to send
//							StringBuilder dataToSend = new StringBuilder();
//							dataToSend.append("InsertReplicate").append("&").append(keytoInsert).append("&").append(valuetoInsert);
//							String concatenatedMessage=dataToSend.toString();
//
//
//							try {
//								Socket specialsocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successorPort1) * 2);
//								DataOutputStream specialdataOutputStream = new DataOutputStream(specialsocket.getOutputStream());
//								specialdataOutputStream.writeUTF(concatenatedMessage);
//								Log.v(TAG, " Special Message sent to " + successorPort1 + " and message is : " + concatenatedMessage);
//
//								Socket specialsocket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successorPort2) * 2);
//								DataOutputStream specialdataOutputStream1 = new DataOutputStream(specialsocket1.getOutputStream());
//								specialdataOutputStream1.writeUTF(concatenatedMessage);
//								Log.v(TAG, " Special Message sent to " + successorPort2 + " and message is : " + concatenatedMessage);
//
//
//							} catch (UnknownHostException e) {
//								Log.e(TAG, "doInBackground: Unknown host exception occurred");
//							} catch (IOException e) {
//								Log.e(TAG, "doInBackground: IO exception occurred 2");
//								e.printStackTrace();
//
//							}
						} else if (operationToPerform.equals("Query")) {
							String keytoQuery = tokenisedData[1];
							String file_name = keytoQuery;
							StringBuilder dataToReturn = new StringBuilder("QueryOutput&");
							try {
								FileInputStream fis = context.openFileInput(file_name);
								InputStreamReader inputStreamReader = new InputStreamReader(fis);
								BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
								StringBuilder sb = new StringBuilder();
								String line;
								while ((line = bufferedReader.readLine()) != null) {
									sb.append(line);
								}
								Log.e("data read", sb.toString());
								dataToReturn.append(file_name).append("&").append(sb.toString());

								fis.close();
							} catch (IOException e) {
								Log.v("Failed to query", keytoQuery);
							}
							messageToClient = dataToReturn.toString();
							try {
								if (messageToClient != null) {
									DataOutputStream dataOutputStream = new DataOutputStream(server.getOutputStream());
									Log.v(TAG, "doInBackground: Writing data to output stream:  " + messageToClient);
									dataOutputStream.writeUTF(messageToClient);
									Log.v(TAG, "doInBackground: data written to client");
								}
							} catch (IOException e1) {
								Log.e(TAG, " IO Exception Occurred. ");
							}
						} else if (operationToPerform.equals("QueryGlobal")) {
							StringBuilder dataToReturn = new StringBuilder("QueryGlobalOutput&");

							context = getContext();
							String fileList[] = context.fileList();
							for (int i = 0; i < fileList.length; i++) {
								String file_name = fileList[i];
								try {
									FileInputStream fis = context.openFileInput(file_name);
									InputStreamReader inputStreamReader = new InputStreamReader(fis);
									BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
									StringBuilder sb = new StringBuilder();
									String line;
									while ((line = bufferedReader.readLine()) != null) {
										sb.append(line);
									}
									Log.e("data read", sb.toString());
									String key = file_name;
									String value = sb.toString();
									//create stringbuilder here
									dataToReturn.append(key).append("-").append(value).append("&");

									fis.close();

								} catch (IOException e) {
									Log.v(TAG, "Failed to query @ ");
								}

							}
							messageToClient = dataToReturn.toString();

							//removing last stary character
							if (messageToClient != null && messageToClient.length() > 0 && messageToClient.charAt(messageToClient.length() - 1) == '&') {
								messageToClient = messageToClient.substring(0, messageToClient.length() - 1);
							}
							try {
								if (messageToClient != null) {
									DataOutputStream dataOutputStream = new DataOutputStream(server.getOutputStream());
									Log.v(TAG, "doInBackground: Writing data to output stream:  " + messageToClient);
									dataOutputStream.writeUTF(messageToClient);
									Log.v(TAG, "doInBackground: data written to client");
								}
							} catch (IOException e1) {
								Log.e(TAG, " IO Exception Occurred. ");
							}

						} else if (operationToPerform.equals("SendDataPre")) {
							//first calculate data to be sent
							//then make string of key value pairs and send
							//Since this is pre predecessor, data to be sent is only data that he himself should have had
							//getFilelist
							context = getContext();
							String fileList[] = context.fileList();
							String destinationPort="";
							StringBuilder replytoOncreate=new StringBuilder("PreData&");
							for (int j = 0; j < fileList.length; j++) {
								String file_name = fileList[j];
								try {
									boolean foundDestinationFlag=false;
									for (int i=0;i<nodesList.size();i++)
									{
										String porttoCheck=nodesList.get(i);
										int comparisonVal=genHash(file_name).compareTo(genHash(porttoCheck));
										if(comparisonVal<=0)
										{
											destinationPort=porttoCheck;
											foundDestinationFlag=true;
											break;
										}
									}
									if (foundDestinationFlag==false) {
										destinationPort = nodesList.get(0);
									}
									Log.v(TAG, "destination port for key : " + file_name + " is :" + destinationPort);
									Log.v(TAG,"my port is "+myPort);

								} catch (NoSuchAlgorithmException e) {
									Log.e(TAG, "doInBackground: Error exception occurred in genhash comparison");
								}
								//now here check if dest port is my port, if true, insert into string;
								if(destinationPort.equals(myPort))
								{
									//Start appendingf to the stringbuilder
									try {
										FileInputStream fis = context.openFileInput(file_name);
										InputStreamReader inputStreamReader = new InputStreamReader(fis);
										BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
										StringBuilder sb = new StringBuilder();
										String line;
										while ((line = bufferedReader.readLine()) != null) {
											sb.append(line);
										}
										Log.e("data read", sb.toString());
										String key = file_name;
										String value = sb.toString();
										//create stringbuilder here
										replytoOncreate.append(key).append("-").append(value).append("&");

										fis.close();

									} catch (IOException e) {
										Log.v(TAG, "Failed to query @ ");
									}
								}




							}
							messageToClient = replytoOncreate.toString();

							//removing last stary character
							if (messageToClient != null && messageToClient.length() > 0 && messageToClient.charAt(messageToClient.length() - 1) == '&') {
								messageToClient = messageToClient.substring(0, messageToClient.length() - 1);
							}
							try {
								if (messageToClient != null) {
									DataOutputStream dataOutputStream = new DataOutputStream(server.getOutputStream());
									Log.v(TAG, "doInBackground: Writing data to output stream:  " + messageToClient);
									dataOutputStream.writeUTF(messageToClient);
									Log.v(TAG, "doInBackground: data written to client");
								}
							} catch (IOException e1) {
								Log.e(TAG, " IO Exception Occurred. ");
							}

						}
						else if (operationToPerform.equals("SendDataSuc")) {
							//first calculate data to be sent
							//then make string of key value pairs and send
							//Since this is pre predecessor, data to be sent is only data that he himself should have had
							//getFilelist
							context = getContext();
							String fileList[] = context.fileList();
							String destinationPort="";
							StringBuilder replytoOncreate=new StringBuilder("SuccData&");
							for (int j = 0; j < fileList.length; j++) {
								String file_name = fileList[j];
								try {
									boolean foundDestinationFlag=false;
									for (int i=0;i<nodesList.size();i++)
									{
										String porttoCheck=nodesList.get(i);
										int comparisonVal=genHash(file_name).compareTo(genHash(porttoCheck));
										if(comparisonVal<=0)
										{
											destinationPort=porttoCheck;
											foundDestinationFlag=true;
											break;
										}
									}
									if (foundDestinationFlag==false) {
										destinationPort = nodesList.get(0);
									}
									Log.v(TAG, "destination port for key : " + file_name + " is :" + destinationPort);
									Log.v(TAG,"my port is "+myPort);

								} catch (NoSuchAlgorithmException e) {
									Log.e(TAG, "doInBackground: Error exception occurred in genhash comparison");
								}
								//now here check if dest port is my port, if true, insert into string;
								if(destinationPort.equals(predecessorport)||destinationPort.equals(prepredecessorport))
								{
									//Start appendingf to the stringbuilder
									try {
										FileInputStream fis = context.openFileInput(file_name);
										InputStreamReader inputStreamReader = new InputStreamReader(fis);
										BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
										StringBuilder sb = new StringBuilder();
										String line;
										while ((line = bufferedReader.readLine()) != null) {
											sb.append(line);
										}
										Log.e("data read", sb.toString());
										String key = file_name;
										String value = sb.toString();
										//create stringbuilder here
										replytoOncreate.append(key).append("-").append(value).append("&");

										fis.close();

									} catch (IOException e) {
										Log.v(TAG, "Failed to query @ ");
									}
								}




							}
							messageToClient = replytoOncreate.toString();

							//removing last stary character
							if (messageToClient != null && messageToClient.length() > 0 && messageToClient.charAt(messageToClient.length() - 1) == '&') {
								messageToClient = messageToClient.substring(0, messageToClient.length() - 1);
							}
							try {
								if (messageToClient != null) {
									DataOutputStream dataOutputStream = new DataOutputStream(server.getOutputStream());
									Log.v(TAG, "doInBackground: Writing data to output stream:  " + messageToClient);
									dataOutputStream.writeUTF(messageToClient);
									Log.v(TAG, "doInBackground: data written to client");
								}
							} catch (IOException e1) {
								Log.e(TAG, " IO Exception Occurred. ");
							}

						}
						else if(operationToPerform.equals("Delete"))
						{
							String keyToDelete=tokenisedData[1];
							Log.v(TAG, "key to be deleted is : " + keyToDelete);
							context = getContext();
							try {
								context.deleteFile(keyToDelete);
							} catch (Exception e) {
								Log.e(TAG, "delete: Error occurred in delete");
							}

						}

					}
				}
			}
			catch(IOException e)
			{
				Log.e(TAG,"IO Exception occurred in servertask"); e.printStackTrace();
			}







			return null;
		}
	}

	private class ClientTask extends AsyncTask<String, String, Void> {
		@Override
		protected Void doInBackground(String... messages) {
			Log.v(TAG, "Inside Doinbackground for client task of port number : " + myPort);

			String operationToPerform = messages[1];
			StringBuilder dataToSend = new StringBuilder();

			if (operationToPerform.equals("Reborn")) {


				StringBuilder messageonCreate=new StringBuilder();
				messageonCreate.append("SendDataPre").append("&").append(myPort);
				try {
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(prepredecessorport) * 2);
					DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
					dataOutputStream.writeUTF(messageonCreate.toString());
					Log.v(TAG, " Special Message sent to " + prepredecessorport + " and message is : " + messageonCreate);

					//wait here for reply//modify  here below
					DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());

					try {
						String messageFromServer = dataInputStream.readUTF();
						Log.v(TAG, "doInBackground: Client Task : message received is :" + messageFromServer);
						if (messageFromServer == null) {
							Log.v(TAG, "doInBackground: server sent nothing ");


						} else {
							Log.v(TAG, "server says that :" + messageFromServer);
							String[] tokenisedData = messageFromServer.split("&");
							if (tokenisedData[0].equals("PreData")) {
								//correct data recieved

								for (int h=1;h<tokenisedData.length;h++)
								{
									String [] tokeniseLvl2=tokenisedData[h].split("-");
									String keyToInsert = tokeniseLvl2[0];
									String valuetoInsert = tokeniseLvl2[1];

									String file_name=keyToInsert;
									String data = valuetoInsert;
									context=getContext();
									try {
										FileOutputStream fos = context.openFileOutput(file_name, Context.MODE_PRIVATE);
										fos.write(data.getBytes());
										fos.close();

									}
									catch(IOException e)
									{
										Log.v("Failed to insert",keyToInsert.toString());
									}

									//String[] row = {keyToQuery, value};
									//matrixCursor.addRow(row);



								}
							}

						}
					} catch (IOException e) {
						Log.v(TAG, "IO Exception occurred in query response");
					}

				} catch (UnknownHostException e) {
					Log.e(TAG, "doInBackground: Unknown host exception occurred");
				} catch (IOException e) {
					Log.e(TAG, "doInBackground: IO exception occurred 2");
					e.printStackTrace();

				}

				StringBuilder messageonCreate2=new StringBuilder();
				messageonCreate2.append("SendDataSuc").append("&").append(myPort);
				try {
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successorPort1) * 2);
					DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
					dataOutputStream.writeUTF(messageonCreate2.toString());
					Log.v(TAG, " Special Message sent to " + successorPort1 + " and message is : " + messageonCreate2);

					//wait here for reply//modify  here below
					DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());

					try {
						String messageFromServer = dataInputStream.readUTF();
						Log.v(TAG, "doInBackground: Client Task : message received is :" + messageFromServer);
						if (messageFromServer == null) {
							Log.v(TAG, "doInBackground: server sent nothing ");


						} else {
							Log.v(TAG, "server says that :" + messageFromServer);
							String[] tokenisedData = messageFromServer.split("&");
							if (tokenisedData[0].equals("SuccData")) {
								//correct data recieved

								for (int h=1;h<tokenisedData.length;h++)
								{
									String [] tokeniseLvl2=tokenisedData[h].split("-");
									String keyToInsert = tokeniseLvl2[0];
									String valuetoInsert = tokeniseLvl2[1];

									String file_name=keyToInsert;
									String data = valuetoInsert;
									context=getContext();
									try {
										FileOutputStream fos = context.openFileOutput(file_name, Context.MODE_PRIVATE);
										fos.write(data.getBytes());
										fos.close();

									}
									catch(IOException e)
									{
										Log.v("Failed to insert",keyToInsert.toString());
									}

									//String[] row = {keyToQuery, value};
									//matrixCursor.addRow(row);



								}
							}

						}
					} catch (IOException e) {
						Log.v(TAG, "IO Exception occurred in query response");
					}

				} catch (UnknownHostException e) {
					Log.e(TAG, "doInBackground: Unknown host exception occurred");
				} catch (IOException e) {
					Log.e(TAG, "doInBackground: IO exception occurred 2");
					e.printStackTrace();

				}


			}






			return null;
		}
	}





	private void balanceChord()
	{
		//http://stackoverflow.com/questions/6957631/sort-java-collection
		Comparator<String> comparator = new Comparator<String>() {
			public int compare(String c1, String c2) {
				try {
					return genHash(c1).compareTo(genHash(c2));
				}
				catch(NoSuchAlgorithmException e)
				{
					Log.e(TAG,"No such algorithm exception while sorting");
				}
				return 0;}
		};
		Collections.sort(nodesList, comparator);



		for (int i=0;i<nodesList.size();i++)
		{
			Log.v(TAG,"Sorted chord node "+i+" is "+nodesList.get(i));
		}

	}
    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
