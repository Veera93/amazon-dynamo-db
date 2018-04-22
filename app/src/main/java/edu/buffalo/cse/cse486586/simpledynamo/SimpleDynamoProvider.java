package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Formatter;

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
import edu.buffalo.cse.cse486586.simpledynamo.SimpleDynamoDbHelper;

public class SimpleDynamoProvider extends ContentProvider {

    private  boolean running;
    private  String myId;
    private  static final String TAG = SimpleDynamoProvider.class.getSimpleName();
    private  SimpleDynamoDbHelper dbHelper;
    private SQLiteDatabase db;
    private  final Object lock = new Object();

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }
	@Override
	public boolean onCreate() {
        running = true;
        /*
         * Calculate the port number that this AVD listens on.
         * It is just a hack that I came up with to get around the networking limitations of AVDs.
         */
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        myId = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);

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
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
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
            while(running) {
                try {
                    server = serverSocket.accept();
                    DataInputStream in = new DataInputStream(server.getInputStream());
                    String msg = in.readUTF();
                    Log.v(TAG, "Incoming message" + msg);
                    if(msg != null) {
                        //ToDo: Code server logic here
                    }
                    server.close();
                } catch (IOException e) {
                    Log.e(TAG, "Exception in server");
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
            String toSendPort = msgs[1];
            //ToDo: Code client logic here
            return null;
        }
    }

    private void sendRequest(String type, String toSendPort, String argument) {
        //ToDo: Any request from client Task goes here
    }

    private void sendResponse(String type, String toSendPort, String argument) {
        //ToDo: Any response from client Task goes here
    }

    private void sendMessage(String port, String message) {
        //Any message to be sent to any participant goes here
        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            //Send data to server
            out.writeUTF(message);
            socket.close();
        } catch (IOException e) {
            Log.e(TAG, "Exception in server");
        }
    }

    @Override
    public void shutdown() {
        super.shutdown();
        running = false;
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

    public void customInsert(String key, String value) {
        //ToDo: Need to revisit
        try{
            Log.v(TAG,"insert handled in custom");
            dbHelper = new SimpleDynamoDbHelper(this.getContext());
            db = dbHelper.getWritableDatabase();
            String sql = "INSERT INTO message ("+SimpleDynamoDataEntry.COLUMN_NAME_KEY+", " +SimpleDynamoDataEntry.COLUMN_NAME_VALUE + ") values('"+key+"', '"+value+"');";
            db.execSQL(sql);
        } catch (Exception e) {
            Log.e(TAG, e.getMessage());
        }

    }

    public String[] customQuery(String key) {
        //ToDo: Need to revisit
        Log.v(TAG,"query "+key);
        Cursor mCursor = null;
        dbHelper = new SimpleDynamoDbHelper(this.getContext());
        db = dbHelper.getReadableDatabase();
        String[] messages = null;
        try {
            if (key.compareTo(SimpleDynamoConfiguration.GLOBAL) == 0) {
                String sql = "SELECT * FROM message";
                mCursor = db.rawQuery(sql, null);
            } else {
                Log.v(TAG, "query handled in custom");
                String sql = "SELECT * FROM message WHERE key = ?";
                String[] selectionArgs = {key};
                mCursor = db.rawQuery(sql, selectionArgs);
            }

            if (mCursor != null) {
                Log.v(TAG, DatabaseUtils.dumpCursorToString(mCursor));
                messages = new String[mCursor.getCount() * 2];
                int counter = 0;
                while (mCursor.moveToNext()) {
                    int valueIndex = mCursor.getColumnIndex(SimpleDynamoDataEntry.COLUMN_NAME_VALUE);
                    int keyIndex = mCursor.getColumnIndex(SimpleDynamoDataEntry.COLUMN_NAME_KEY);
                    String k = mCursor.getString(keyIndex);
                    String v = mCursor.getString(valueIndex);
                    messages[counter] = k;
                    ++counter;
                    messages[counter] = v;
                    ++counter;
                }
            }
        } catch (Exception e) {
            Log.e(TAG, e.getMessage());
        } finally {
            mCursor.close();
            return messages;
        }
    }

    public int customDelete(String selection) {
        //ToDo: Need to revisit
        Log.v(TAG, "Deleting Locally");
        int rowsAffected = 0;
        dbHelper = new SimpleDynamoDbHelper(this.getContext());
        db = dbHelper.getWritableDatabase();
        if(selection.compareTo(SimpleDynamoConfiguration.GLOBAL) == 0 || selection.compareTo(SimpleDynamoConfiguration.LOCAL) == 0) {
            rowsAffected = db.delete(SimpleDynamoDataEntry.TABLE_NAME, null, null);
        } else {
            String mselection = SimpleDynamoDataEntry.COLUMN_NAME_KEY + " LIKE ?";
            String[] mselectionArgs = { selection };
            // Issue SQL statement.
            rowsAffected = db.delete(SimpleDynamoDataEntry.TABLE_NAME, mselection, mselectionArgs);
        }
        return rowsAffected;
    }

    public Cursor localQuery(String[] projection, String selection, String[] mSelectArg, String sortOrder) {
        //ToDo: Need to revisit
        Log.v(TAG, "Query Handled locally");
        dbHelper = new SimpleDynamoDbHelper(this.getContext());
        db = dbHelper.getReadableDatabase();
        Cursor cursor = null;
        try {
            cursor = db.query(
                    SimpleDynamoDataEntry.TABLE_NAME,   // The table to query
                    projection,                     // The columns to return
                    selection,                     // The columns for the WHERE clause
                    mSelectArg,                     // The values for the WHERE clause
                    null,                  // don't group the rows
                    null,                   // don't filter by row groups
                    sortOrder                      // The sort order
            );
        } catch (Exception e) {
            Log.e(TAG, e.getMessage());
        }
        return cursor;
    }
}
