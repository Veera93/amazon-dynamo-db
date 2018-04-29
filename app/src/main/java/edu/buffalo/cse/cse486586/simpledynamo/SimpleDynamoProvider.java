package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import edu.buffalo.cse.cse486586.simpledynamo.SimpleDynamoSchema.SimpleDynamoDataEntry;

public class SimpleDynamoProvider extends ContentProvider {

    private  boolean running;
    private  String myId;
    private  static final String TAG = SimpleDynamoProvider.class.getSimpleName();
    private  SimpleDynamoDbHelper dbHelper;
    private SQLiteDatabase db;
    private BlockingQueue<String> blockingQueue = new ArrayBlockingQueue<String>(1);
    private ArrayList<String> pendingWrites = new ArrayList<String>();
    private Map<String, Integer> insertResponse = new HashMap<String, Integer>();
    private Object lock = new Object();
    private boolean isRecovering = false;
    private Object readLock = new Object();
    private String queryResponses = "";
    private int queryCount = 0;

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }
	@Override
	public boolean onCreate() {
        Log.i(TAG,"On create called in CP");
        running = true;
        /*
         * Calculate the port number that this AVD listens on.
         * It is just a hack that I came up with to get around the networking limitations of AVDs.
         */
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        myId = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        Log.v(TAG, "id "+myId);
        try {
            /*
             * Create a server socket as well as a thread (AsyncTask) that listens on the server
             * port.
             *
             * AsyncTask is a simplified thread construct that Android provides.
             */
            ServerSocket serverSocket = new ServerSocket(SimpleDynamoConfiguration.SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return true;
        }

        return true;
	}

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        /*
         * values will have two columns (a key
         * column and a value column) and one row that contains the actual (key, value) pair to be
         * inserted.
         *
         * Used SQLite for database
         */
        synchronized (lock) {
            while(isRecovering) {
                try {
                    lock.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        try {
            Log.i(TAG,"Content provider insert called "+values.toString());
            String key = (String) values.get(SimpleDynamoDataEntry.COLUMN_NAME_KEY);
            String value = (String) values.get(SimpleDynamoDataEntry.COLUMN_NAME_VALUE);
            String coordinatorId = findOwner(key);
            Integer coordinatorPort = Integer.parseInt(coordinatorId) * 2;
            String token = key+SimpleDynamoConfiguration.ARG_DELIMITER+value+SimpleDynamoConfiguration.ARG_DELIMITER+myId;
            SimpleDynamoRequest request = new SimpleDynamoRequest(coordinatorId, SimpleDynamoRequest.Type.COORDINATOR, token);
            String args = request.toString();
            sendToCoordinator(coordinatorPort, args);
            String output = blockingQueue.poll(SimpleDynamoConfiguration.TIMEOUT_BLOCKING, TimeUnit.MILLISECONDS);
            blockingQueue = new ArrayBlockingQueue<String>(1);
            //If timeout expires then output will be null
            Log.v(TAG,"Blocking queue released: "+ output);
            if(output == null) {
                String newCoordinator = findNewCoordinator(coordinatorId);
                Integer newCoordinatorPort = Integer.parseInt(newCoordinator) * 2;
                Log.v(TAG, "Resending inserting again for "+key+" to "+newCoordinatorPort);
                sendToCoordinator(newCoordinatorPort, args);
                output = blockingQueue.poll(SimpleDynamoConfiguration.TIMEOUT_BLOCKING, TimeUnit.MILLISECONDS);
                blockingQueue = new ArrayBlockingQueue<String>(1);
                Log.v(TAG,"Second Blocking queue released: "+ output);
            }
        } catch (Exception e) {
            Log.e(TAG, "Exception in Sending inserting again");
        } finally {
            Log.v(TAG, "Insert done "+ values.get(SimpleDynamoDataEntry.COLUMN_NAME_KEY));
            return uri;
        }
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
        synchronized (lock) {
            while(isRecovering) {
                try {
                    lock.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        Log.i(TAG, "Content provider query called "+ selection);
        dbHelper = new SimpleDynamoDbHelper(this.getContext());
        db = dbHelper.getReadableDatabase();
        MatrixCursor matrixCursor = new MatrixCursor(new String[]{SimpleDynamoDataEntry.COLUMN_NAME_KEY, SimpleDynamoDataEntry.COLUMN_NAME_VALUE});
        Cursor cursor = null;
        try {
            if(selection.compareTo(SimpleDynamoConfiguration.GLOBAL) == 0) {
                queryCount = 5;
                for(String id: SimpleDynamoConfiguration.PORTS) {
                    try {
                        Integer port = (Integer.parseInt(id) * 2);
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), port);
                        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                        //socket.setSoTimeout(SimpleDynamoConfiguration.ACK_TIME);
                        //socket.setTcpNoDelay(false);
                        SimpleDynamoRequest ack = new SimpleDynamoRequest(myId, SimpleDynamoRequest.Type.ACK, "ack");
                        out.writeUTF(ack.toString());

                        DataInputStream in = new DataInputStream(socket.getInputStream());
                        String sAck = in.readUTF();
                        Log.v(TAG, "Got response ack: "+sAck);
                        if(sAck.equals("RECOVERING")) {
                            throw new IOException();
                        }
                        socket.close();
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), port);
                        out = new DataOutputStream(socket.getOutputStream());
                        SimpleDynamoRequest request = new SimpleDynamoRequest(myId, SimpleDynamoRequest.Type.QUERY, SimpleDynamoConfiguration.GLOBAL);
                        String message = request.toString();
                        //Send data to server
                        out.writeUTF(message);
                        socket.close();
                    } catch (IOException e) {
                        Log.e(TAG, "IO Exception in query "+id);
                        queryCount = queryCount - 1;
                    } catch (Exception e) {
                        Log.e(TAG, "Exception in query 1");
                    }
                }

                synchronized (readLock) {
                    readLock.wait(1000);
                    Log.v(TAG, "Read lock released: "+queryCount);
                }

                String[] response = queryResponses.split(SimpleDynamoConfiguration.ARG_DELIMITER);
                queryResponses = "";
                if(response.length > 0) {
                    for(int i = 0; i < response.length - 1 ; i++) {
                        matrixCursor.addRow(new String[] {response[i], response[i+1]});
                        i++;
                    }
                }
                cursor = matrixCursor;
            } else if(selection.compareTo(SimpleDynamoConfiguration.LOCAL) == 0) {
                cursor = localQuery(projection,null,null, sortOrder);
            } else if(selection != null) {
                String coordinator = findOwner(selection);
                String readOwner = findReadOwner(coordinator);
                cursor = remoteQuery(readOwner, selection, true);
            }
        } catch (Exception e) {
            Log.e(TAG," " + Arrays.toString(e.getStackTrace()));
            Log.e(TAG, "Exception in Query 3");
        } finally {
            Log.v(TAG, "Returning cursor: "+ DatabaseUtils.dumpCursorToString(cursor));
            return cursor;
        }
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        String mSelection = SimpleDynamoDataEntry.COLUMN_NAME_KEY + " LIKE ?";
        String key = (String) values.get(SimpleDynamoDataEntry.COLUMN_NAME_KEY);
        String[] mSelectionArgs = { key };

        int count = db.update(
                SimpleDynamoDataEntry.TABLE_NAME,
                values,
                mSelection,
                mSelectionArgs);

        return count;
    }

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
        Log.i(TAG, "Content provider delete called "+ selection);
        ContentValues values = new ContentValues();
        values.put(SimpleDynamoDataEntry.COLUMN_NAME_KEY, selection);
        values.put(SimpleDynamoDataEntry.COLUMN_NAME_VALUE, SimpleDynamoConfiguration.SOFT_DELETE);
        insert(uri, values);
        return 0;
	}


    /*
     * ServerTask is an AsyncTask that should handle incoming messages. It is created by
     * ServerTask.executeOnExecutor() call.
     */
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            Socket server = null;
            Log.v(TAG,"BEFORE: "+isRecovering);
            //isRecovering = true;
            Log.v(TAG,"RECOVERING: "+isRecovering);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, SimpleDynamoConfiguration.RECOVERY);
            while(running) {
                try {
                    server = serverSocket.accept();
                    DataInputStream in = new DataInputStream(server.getInputStream());
                    String msg = in.readUTF();
                    Log.v(TAG, "Incoming message " + msg);
                    if(msg != null) {
                        String[] parsedMsg = msg.split(SimpleDynamoConfiguration.DELIMITER, 3);
                        String type = parsedMsg[1];
                        if(type.equals(SimpleDynamoRequest.Type.COORDINATOR)) {
                            /****
                                * In the absence of failure coordinator will be equal to my node and hence send it to two replicas.
                                * But, in case of the coordinator failed and it is passed to its next node and hence coordinator will
                                * not be my node so send only to my successor
                            *****/
                            if(isRecovering == false) {
                                //Insert in my local
                                String coordinator = parsedMsg[0];
                                //Insert and Replicating
                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, SimpleDynamoRequest.Type.REPLICATE, coordinator , parsedMsg[2]);
                            } else {
                                pendingWrites.add(parsedMsg[2]);
                                Log.i(TAG,"ADDING: "+parsedMsg[2]);
                            }
                        } else if (type.equals(SimpleDynamoRequest.Type.REPLICATE)) {
                            if(isRecovering == false) {
                                String[] pair = parsedMsg[2].split(SimpleDynamoConfiguration.ARG_DELIMITER);
                                //Reply to coordinator
                                String coordinator = parsedMsg[0];
                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, SimpleDynamoResponse.Type.REPLICATE, coordinator , parsedMsg[2]);
                            } else {
                                pendingWrites.add(parsedMsg[2]);
                                Log.i(TAG,"ADDING: "+parsedMsg[2]);
                            }

                        } else if (type.equals(SimpleDynamoResponse.Type.REPLICATE) && isRecovering == false) {
                            String token = parsedMsg[2];
                            Integer count = insertResponse.get(token);
                            Log.v(TAG, "Server insertResponse : "+insertResponse.get(token)+" for "+token);
                            if(count != null) {
                                if(count.equals(1)) {
                                    String[] tokenArr = token.split(SimpleDynamoConfiguration.ARG_DELIMITER);
                                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, SimpleDynamoResponse.Type.INSERT, tokenArr[2], parsedMsg[2]);
                                    insertResponse.remove(token);
                                } else {
                                    insertResponse.put(token, insertResponse.get(token) - 1);
                                }
                            } else {
                                Log.v("ALERT", token);
                            }
                        } else if(type.equals(SimpleDynamoResponse.Type.INSERT) && isRecovering == false) {
                            blockingQueue.put(parsedMsg[2]);
                        } else if(type.equals(SimpleDynamoRequest.Type.QUERY) && isRecovering == false) {
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, type, parsedMsg[0], parsedMsg[2]);
                        } else if(type.equals(SimpleDynamoResponse.Type.QUERY)) {
                            if(queryResponses.equals("")) {
                                queryResponses = parsedMsg[2];
                            } else {
                                queryResponses = queryResponses + SimpleDynamoConfiguration.ARG_DELIMITER +parsedMsg[2];
                            }
                            queryCount = queryCount - 1;
                            if(queryCount == 0) {
                                synchronized (readLock) {
                                    readLock.notifyAll();
                                }
                            }
                        } else if(type.equals(SimpleDynamoRequest.Type.ACK)) {
                            DataOutputStream out = new DataOutputStream(server.getOutputStream());
                            if(isRecovering == false) {
                                out.writeUTF("ALIVE");
                            } else {
                                out.writeUTF("RECOVERING");
                            }

                        }
                    }
                    server.close();
                } catch (IOException e) {
                    Log.e(TAG, "Exception in server");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            return null;
        }

        protected void onProgressUpdate(String...strings) {
            /*
             * The following code displays what is received in doInBackground()
             */
        }
    }

    /***
     * ClientTask is an AsyncTask that should send a string over the network.
     * It is created by ClientTask.executeOnExecutor() call
     */
    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            String type = msgs[0];
            if(type.equals(SimpleDynamoRequest.Type.REPLICATE)) {
                String coordinatorId = msgs[1];
                String token = msgs[2];
                String pair[] = token.split(SimpleDynamoConfiguration.ARG_DELIMITER);
                customInsert(pair[0],pair[1]);
                SimpleDynamoRequest request = new SimpleDynamoRequest(myId, SimpleDynamoRequest.Type.REPLICATE, token);
                String[] preList = getPreferenceList(SimpleDynamoConfiguration.PORTS, coordinatorId);
                String args = request.toString();
                if(coordinatorId.equals(myId)) {
                    sendReplicateMessages(new String[] { preList[1], preList[2] } ,args);
                } else {
                    sendReplicateMessages(new String[] { preList[2] }, args);
                }
            } else if(type.equals(SimpleDynamoResponse.Type.REPLICATE)) {
                SimpleDynamoResponse response = new SimpleDynamoResponse(myId, SimpleDynamoResponse.Type.REPLICATE, msgs[2]);
                String[] pair =  msgs[2].split(SimpleDynamoConfiguration.ARG_DELIMITER);
                customInsert(pair[0],pair[1]);
                String arg = response.toString();
                sendMessages(new String[] {msgs[1]}, arg);
            } else if(type.equals(SimpleDynamoResponse.Type.INSERT)) {
                SimpleDynamoResponse response = new SimpleDynamoResponse(myId, SimpleDynamoResponse.Type.INSERT, msgs[2]);
                String arg = response.toString();
                sendMessages(new String[] {msgs[1]}, arg);
            } else if(type.equals(SimpleDynamoConfiguration.RECOVERY)) {
                //recoverFromFailure();
            } else if(type.equals(SimpleDynamoRequest.Type.QUERY)) {
                String key = msgs[2];
                String[] returnValue = customQuery(key);
                StringBuilder values = new StringBuilder();
                for(int i = 0; i < returnValue.length - 1 ; i++) {
                    values.append(returnValue[i]);
                    values.append(SimpleDynamoConfiguration.ARG_DELIMITER);
                    values.append(returnValue[i+1]);
                    values.append(SimpleDynamoConfiguration.ARG_DELIMITER);
                    i++;
                }
                if(values.length() > 0)
                    values.deleteCharAt(values.length() - 1);
                SimpleDynamoResponse response = new SimpleDynamoResponse(myId, SimpleDynamoResponse.Type.QUERY, values.toString());
                sendMessages(new String[] {msgs[1]}, response.toString());
            }
            return null;
        }
    }


    private int sendMessages(String[] ids, String message) {
        //Any message to be sent to any participant goes here
        int count = ids.length;
        for(String id : ids) {
            try {
                Integer port = (Integer.parseInt(id) * 2);
                Log.v(TAG,"Outgoing message to "+ port+" message "+message);
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), port);
                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                //socket.setSoTimeout(SimpleDynamoConfiguration.TIMEOUT_TIME);
                //socket.setTcpNoDelay(false);
                //Send data to server
                out.writeUTF(message);
                socket.close();
            } catch (SocketException e) {
                Log.e(TAG, "ClientTask Socket Exception");
                --count;
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException ");
                --count;
            } catch (Exception e) {
                Log.e(TAG, e.getMessage());
                --count;
            }
        }
        return count;
    }

    private void sendReplicateMessages(String[] ids, String message) {
        //Any message to be sent to any participant goes here
        String[] parsedMsg = message.split(SimpleDynamoConfiguration.DELIMITER);
        insertResponse.put(parsedMsg[2], ids.length);
        for(String id : ids) {
            try {
                Integer port = (Integer.parseInt(id) * 2);
                Log.v(TAG,"Outgoing replicate message to "+ port+" message "+message);
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), port);
                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                //socket.setSoTimeout(SimpleDynamoConfiguration.ACK_TIME);
                //socket.setTcpNoDelay(false);
                SimpleDynamoRequest ack = new SimpleDynamoRequest(myId, SimpleDynamoRequest.Type.ACK, "ack");
                out.writeUTF(ack.toString());
                DataInputStream in = new DataInputStream(socket.getInputStream());
                String sAck = in.readUTF();
                Log.v(TAG, "Got response ack: "+sAck);
                if(sAck.equals("RECOVERING")) {
                    throw new IOException();
                }
                socket.close();
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), port);
                out = new DataOutputStream(socket.getOutputStream());
                //socket.setSoTimeout(SimpleDynamoConfiguration.TIMEOUT_TIME);
                //Send data to server
                out.writeUTF(message);
                socket.close();
            } catch (SocketException e) {
                Log.e(TAG, "ClientTask Socket Exception");
                insertResponse.put(parsedMsg[2], insertResponse.get(parsedMsg[2]) - 1);
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException ");
                insertResponse.put(parsedMsg[2], insertResponse.get(parsedMsg[2]) - 1);
            } catch (Exception e) {
                Log.e(TAG, e.getMessage());
                insertResponse.put(parsedMsg[2], insertResponse.get(parsedMsg[2]) - 1);
            }
        }
    }

    private void sendToCoordinator(Integer port, String message) {
            //Any message to be sent to any coordinator goes here and it will be blocked using the socket
            Log.v(TAG,"Outgoing message to "+ port+" message "+message);
            try {
                //Send data to server
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), port);
                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                out.writeUTF(message);
                socket.close();
            } catch (SocketException e) {
                Log.e(TAG, "ClientTask Socket Exception");
                //ToDo: Call with port's successor for failure handling
            } catch (IOException e) {
                Log.e(TAG, "Exception in server");
                //ToDo: Call with port's successor for failure handling
            } catch (Exception e) {
                Log.e(TAG, e.getMessage());
                Log.e(TAG, "Exception in Send to co ordinator");
            }
    }

    @Override
    public void shutdown() {
        super.shutdown();
        running = false;
    }

    private void customInsert(String key, String value) {
        ContentValues values = new ContentValues();
        values.put(SimpleDynamoDataEntry.COLUMN_NAME_KEY, key);
        values.put(SimpleDynamoDataEntry.COLUMN_NAME_VALUE, value);
        Log.v(TAG,"Insert Custom "+ key+" "+value);
        dbHelper = new SimpleDynamoDbHelper(this.getContext());
        db = dbHelper.getWritableDatabase();
        db.insertWithOnConflict (SimpleDynamoDataEntry.TABLE_NAME,
                null,
                values, SQLiteDatabase.CONFLICT_REPLACE);
    }

    private String[] customQuery(String key) {
        //ToDo: Need to revisit
        Log.v(TAG,"Query Custom "+key);
        Cursor mCursor = null;
        dbHelper = new SimpleDynamoDbHelper(this.getContext());
        db = dbHelper.getReadableDatabase();
        String[] messages = null;
        try {
            if (key.compareTo(SimpleDynamoConfiguration.GLOBAL) == 0) {
                String sql = "SELECT * FROM "+ SimpleDynamoDataEntry.TABLE_NAME;
                mCursor = db.rawQuery(sql, null);
            } else {
                String sql = "SELECT * FROM "+SimpleDynamoDataEntry.TABLE_NAME+" WHERE key = ?";
                String[] selectionArgs = {key};
                mCursor = db.rawQuery(sql, selectionArgs);
            }

            if (mCursor != null) {
                //Log.v(TAG, DatabaseUtils.dumpCursorToString(mCursor));
                messages = new String[mCursor.getCount() * 2];
                int counter = 0;
                while (mCursor.moveToNext()) {
                    int valueIndex = mCursor.getColumnIndex(SimpleDynamoDataEntry.COLUMN_NAME_VALUE);
                    int keyIndex = mCursor.getColumnIndex(SimpleDynamoDataEntry.COLUMN_NAME_KEY);
                    String k = mCursor.getString(keyIndex);
                    String v = mCursor.getString(valueIndex);
                    if(!v.equals(SimpleDynamoConfiguration.SOFT_DELETE)) {
                        messages[counter] = k;
                        ++counter;
                        messages[counter] = v;
                        ++counter;
                    }
                }
            }
        } catch (Exception e) {
            Log.e(TAG, e.getMessage());
            Log.e(TAG, "Exception in custom Query");
        } finally {
            mCursor.close();
            return messages;
        }
    }

    private Cursor localQuery(String[] projection, String selection, String[] mSelectArg, String sortOrder) {
        //ToDo: Need to revisit
        Log.v(TAG, "Query @ Custom "+selection);
        dbHelper = new SimpleDynamoDbHelper(this.getContext());
        db = dbHelper.getReadableDatabase();
        Cursor cursor = null;
        try {
            cursor = db.query(
                    SimpleDynamoDataEntry.TABLE_NAME,   // The table to query
                    projection,                     // The columns to return
                    SimpleDynamoDataEntry.COLUMN_NAME_VALUE+" NOT IN ('"+SimpleDynamoConfiguration.SOFT_DELETE+"')",                     // The columns for the WHERE clause
                    mSelectArg,                     // The values for the WHERE clause
                    null,                  // don't group the rows
                    null,                   // don't filter by row groups
                    sortOrder                      // The sort order
            );
        } catch (Exception e) {
            Log.e(TAG, e.getMessage());
            Log.e(TAG, "Exception in local Query");
        }
        return cursor;
    }

    private MatrixCursor remoteQuery(String readOwnerId, String selection, boolean retry) {
        queryCount = 1;
        Integer readOwnerPort = Integer.parseInt(readOwnerId) * 2;
        Log.v(TAG, "Calling Remote Query for "+ selection+ "to "+readOwnerPort);
        MatrixCursor matrixCursor = new MatrixCursor(new String[]{SimpleDynamoDataEntry.COLUMN_NAME_KEY, SimpleDynamoDataEntry.COLUMN_NAME_VALUE});
        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), readOwnerPort);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            //socket.setSoTimeout(SimpleDynamoConfiguration.ACK_TIME);
            //socket.setTcpNoDelay(false);
            SimpleDynamoRequest ack = new SimpleDynamoRequest(myId, SimpleDynamoRequest.Type.ACK, "ack");
            out.writeUTF(ack.toString());

            DataInputStream in = new DataInputStream(socket.getInputStream());
            String sAck = in.readUTF();
            Log.v(TAG, "Got response ack: "+sAck);
            if(sAck.equals("RECOVERING")) {
                throw new IOException();
            }
            socket.close();
            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), readOwnerPort);
            out = new DataOutputStream(socket.getOutputStream());
            SimpleDynamoRequest request = new SimpleDynamoRequest(myId, SimpleDynamoRequest.Type.QUERY, selection);
            String message = request.toString();
            //Send data to server
            out.writeUTF(message);
            socket.close();

        } catch (IOException e) {
            Log.e(TAG, "IO Exception in Query "+readOwnerPort+ " for "+selection);
            if(!retry) {
                Log.v(TAG, "NO RETRIES"+readOwnerPort+ " for "+selection);
            }
            if(retry) {
                String newReadOwner = findNewReadOwner(readOwnerId);
                matrixCursor = remoteQuery(newReadOwner, selection, false);
            }

        } catch (Exception e) {
            Log.e(TAG, " E "+e.getMessage());
            Log.e(TAG, "Exception in Query 2");
        } finally {
            synchronized (readLock) {
                try {
                    readLock.wait(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                Log.v(TAG, "Read lock released: "+queryCount);
            }

            String[] response = queryResponses.split(SimpleDynamoConfiguration.ARG_DELIMITER);
            queryResponses = "";
            if(response.length > 0) {
                for(int i = 0; i < response.length - 1 ; i++) {
                    matrixCursor.addRow(new String[] {response[i], response[i+1]});
                    i++;
                }
            }
            return matrixCursor;
        }
    }

    private static String[] getPreferenceList(String[] list, String node) {
        int location = 0;
        for(int i=0;i<list.length;i++) {
            if(list[i].equals(node)) {
                location = i;
                break;
            }
        }
        if(location == list.length - 1) {
            return new String[] { list[location], list[0], list[1] };
        } else if (location == list.length - 2) {
            return new String[] { list[location], list[location + 1], list[0] };
        } else {
            return new String[] { list[location], list[location+1], list[location+2]};
        }
    }

    private static String[] getPredSucc(String[] list, String node) {
        int location = 0;
        for(int i=0;i<list.length;i++) {
            if(list[i].equals(node)) {
                location = i;
                break;
            }
        }
        if(location == 0) {
            return new String[] {list[list.length - 1], list[1]};
        } else if (location == list.length - 1) {
            return new String[] {list[list.length - 2], list[0]};
        } else {
            return new String[] {list[location - 1], list[location + 1]};
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

    private boolean isOwner(String key, String predecessor, String successor, String current) {
        String hashedId = null;
        String hashedSuccessor = null;
        String hashedPredecessor = null;
        String myHashedId = null;
        try {
            hashedId = genHash(key);
            hashedSuccessor = genHash(successor);
            hashedPredecessor = genHash(predecessor);
            myHashedId = genHash(current);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        int nodeKey = hashedId.compareTo(myHashedId);
        int succPred = hashedSuccessor.compareTo(hashedPredecessor);
        int nodePred = myHashedId.compareTo(hashedPredecessor);
        int keyPred = hashedId.compareTo(hashedPredecessor);

        if(nodeKey == 0) {
            return true;
        } else if(succPred == 0 && nodePred == 0) {
            return true;
        } else if(nodePred < 0) {
            if(keyPred > 0 && nodeKey > 0) {
                return true;
            } else if(keyPred < 0 && nodeKey < 0) {
                return true;
            }
        } else if(keyPred > 0 && nodeKey < 0) {
            return true;
        }

        return false;
    }

    private String findOwner(String key) {
        String[] predAndSucc;
        for(String port: SimpleDynamoConfiguration.PORTS) {
            predAndSucc = getPredSucc(SimpleDynamoConfiguration.PORTS, port);
            if(isOwner(key, predAndSucc[0], predAndSucc[1], port)) {
                return port;
            }
        }
        return null;
    }

    private String findReadOwner(String coordinator) {
        String readOwner;
        int location = 0;
        for(int i=0; i<SimpleDynamoConfiguration.PORTS.length; i++) {
            if(SimpleDynamoConfiguration.PORTS[i].equals(coordinator)) {
                location = i;
                break;
            }
        }
        int loc = (location + 2) % 5;
        readOwner = SimpleDynamoConfiguration.PORTS[loc];
        return readOwner;
    }

    private String findNewReadOwner(String currentReadOwner) {
        int location = 0;
        for (int i = 0; i < SimpleDynamoConfiguration.PORTS.length; i++) {
            if (SimpleDynamoConfiguration.PORTS[i].equals(currentReadOwner)) {
                location = i;
                break;
            }
        }
        int loc = location - 1;

        return loc >- 1 ? SimpleDynamoConfiguration.PORTS[loc]: SimpleDynamoConfiguration.PORTS[SimpleDynamoConfiguration.PORTS.length -1];
    }

    private String findNewCoordinator(String coordinator) {
        int location = 0;
        for (int i = 0; i < SimpleDynamoConfiguration.PORTS.length; i++) {
            if (SimpleDynamoConfiguration.PORTS[i].equals(coordinator)) {
                location = i;
                break;
            }
        }
        int loc = location + 1;

        return loc > 4 ? SimpleDynamoConfiguration.PORTS[0]:SimpleDynamoConfiguration.PORTS[loc];
    }

    private void recoverFromFailure() {
        Log.i(TAG, "Recovering :"+ myId);
        String id = findReadOwner(myId);
        MatrixCursor matrixCursor = new MatrixCursor(new String[]{SimpleDynamoDataEntry.COLUMN_NAME_KEY, SimpleDynamoDataEntry.COLUMN_NAME_VALUE});
        try {
            Log.v(TAG, "Querying for my copy");
            Integer port = Integer.parseInt(id) * 2;
            Log.v(TAG, "Found port "+port);
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), port);
            Log.v(TAG, "Contacting "+port);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            //socket.setSoTimeout(SimpleDynamoConfiguration.TIMEOUT_TIME);
            //socket.setTcpNoDelay(false);
            SimpleDynamoRequest request = new SimpleDynamoRequest(myId, SimpleDynamoRequest.Type.QUERY, SimpleDynamoConfiguration.GLOBAL);
            String message = request.toString();
            //Send data to server
            out.writeUTF(message);
            Log.v(TAG, "Sent request");
            //Receive data from server
            DataInputStream in = new DataInputStream(socket.getInputStream());
            String reply = in.readUTF();
            Log.v(TAG, "Received response");
            String[] oResponse = reply.split(SimpleDynamoConfiguration.ARG_DELIMITER);
            socket.close();
            Log.v(TAG, "Querying for replica");
            id = findNewReadOwner(myId);
            String[] pred = getPreList(myId);
            port = (Integer.parseInt(id) * 2);
            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), port);
            out = new DataOutputStream(socket.getOutputStream());
            //socket.setSoTimeout(SimpleDynamoConfiguration.TIMEOUT_TIME);
            //socket.setTcpNoDelay(false);
            request = new SimpleDynamoRequest(myId, SimpleDynamoRequest.Type.QUERY, SimpleDynamoConfiguration.GLOBAL);
            message = request.toString();
            Log.v(TAG, "Querying request");
            //Send data to server
            out.writeUTF(message);

            //Receive data from server
            in = new DataInputStream(socket.getInputStream());
            reply = in.readUTF();
            Log.v(TAG, "Querying response");
            String[] rResponse = reply.split(SimpleDynamoConfiguration.ARG_DELIMITER);
            if(rResponse.length > 0) {
                for(int i = 0; i < rResponse.length - 1 ; i++) {
                    if(findOwner(rResponse[i]).equals(pred[0]) || findOwner(rResponse[i]).equals(pred[1])) {
                        matrixCursor.addRow(new String[] {rResponse[i], rResponse[i+1]});
                    }
                    i++;
                }
            }

            if(oResponse.length > 0) {
                for(int i = 0; i < oResponse.length - 1 ; i++) {
                    if(findOwner(oResponse[i]).equals(myId)) {
                        matrixCursor.addRow(new String[] {oResponse[i], oResponse[i+1]});
                    }
                    i++;
                }
            }

            socket.close();
            while (matrixCursor.moveToNext()) {
                int valueIndex = matrixCursor.getColumnIndex(SimpleDynamoDataEntry.COLUMN_NAME_VALUE);
                int keyIndex = matrixCursor.getColumnIndex(SimpleDynamoDataEntry.COLUMN_NAME_KEY);
                String k = matrixCursor.getString(keyIndex);
                String v = matrixCursor.getString(valueIndex);
                if(!v.equals(SimpleDynamoConfiguration.SOFT_DELETE) && !v.equals("null")) {
                    customInsert(k,v);
                }
            }
        } catch (IOException e) {
            Log.e(TAG, "IO Exception in StartUp "+id);
        } catch (Exception e) {
            Log.e(TAG, "Exception in query recovery");
        } finally {
            isRecovering = false;
            for(String i: pendingWrites) {
                String[] token = i.split(SimpleDynamoConfiguration.ARG_DELIMITER);
                Log.v(TAG, "Inserting key: "+token[0]+"value: "+token[1]);
                customInsert(token[0], token[1]);
            }
            pendingWrites.clear();
            synchronized (lock) {
                lock.notifyAll();
            }
            Log.i(TAG,"Recovery Done");
        }
    }

    public static String[] getPreList(String node) {
        int location = 0;
        for(int i=0;i<SimpleDynamoConfiguration.PORTS.length;i++) {
            if(SimpleDynamoConfiguration.PORTS[i].equals(node)) {
                location = i;
                break;
            }
        }
        if(location == 0) {
            return new String[] { SimpleDynamoConfiguration.PORTS[SimpleDynamoConfiguration.PORTS.length - 1], SimpleDynamoConfiguration.PORTS[SimpleDynamoConfiguration.PORTS.length - 2] };
        } else if (location == 1) {
            return new String[] { SimpleDynamoConfiguration.PORTS[0], SimpleDynamoConfiguration.PORTS[SimpleDynamoConfiguration.PORTS.length - 1] };
        } else {
            return new String[] {  SimpleDynamoConfiguration.PORTS[location-1], SimpleDynamoConfiguration.PORTS[location-2]};
        }
    }

}
