package edu.buffalo.cse562.BDBIndexers;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.*;
import com.sleepycat.je.SecondaryKeyCreator;
import edu.buffalo.cse562.BDBIndexers.LineItemHelper.MyLineItemTupleBinder;
import edu.buffalo.cse562.BDBIndexers.LineItemHelper.SecondaryKeyCreator_LineItem;
import edu.buffalo.cse562.DTO.ColumnDetail;
import edu.buffalo.cse562.DTO.ConfigManager;
import edu.buffalo.cse562.DTO.DataRow;
import edu.buffalo.cse562.DTO.Datum;
import edu.buffalo.cse562.Main;
import edu.buffalo.cse562.Operators.ScanOperatorPlain;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.schema.Table;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by keno on 4/28/15.
 */
public class BDBCreator {
    Environment myDbEnvironment = null;
    EnvironmentConfig envConfig = null;
    String envLocation = "";
    String tableName;
    DatabaseConfig dbConfig;
    Database myDB;
    SecondaryDatabase secDB = null;
    int secIndexPos;
    HashMap<Integer, String> tMap;
    ArrayList<Integer> keyPositions;

    public BDBCreator(String tableName, ArrayList<Integer> keyPositions, int secondaryIndex) {
        /* Creates and loads a BDB table with a secondary index on the column defined by*/
        this.tableName = tableName.toLowerCase();
        this.keyPositions = keyPositions;
        this.secIndexPos = secondaryIndex;
        this.tMap = Main.indexTypeMaps.get(this.tableName);
        setEnvironment();

        dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        myDB = myDbEnvironment.openDatabase(null, this.tableName, dbConfig);
        SecondaryConfig secConfig = new SecondaryConfig();
        secConfig.setAllowCreate(true);
        secConfig.setSortedDuplicates(true);

        createSecDB(secConfig);
        setConfig(dbConfig, secConfig);

        loadData();

        if (secDB != null) {
            secDB.close();
        }
        if (myDB != null) {
            myDB.close();
        }

        closeEnvironment();
    }

    public BDBCreator(String tableName, ArrayList<Integer> keyPositions) {
        /* Creates and loads a BDB table without a secondary index*/
        this.tableName = tableName.toLowerCase();
        this.keyPositions = keyPositions;
        this.tMap = Main.indexTypeMaps.get(this.tableName);
        setEnvironment();

        dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        myDB = myDbEnvironment.openDatabase(null, this.tableName, dbConfig);

        loadData();

        if (myDB != null) {
            myDB.close();
        }
        closeEnvironment();
    }

    private void createSecDB(SecondaryConfig secConfig) {
        TupleBinding<DataRow> tBinder = new GenericTupleBinder(tMap);
        SecondaryKeyCreator secKeyCreator = new GenericSecKeyCreator(tBinder, secIndexPos, tMap);

        // Get a secondary object and set the key creator on it.
        secConfig.setKeyCreator(secKeyCreator);
        String secDbName = tableName + "-secDB";
        secDB = myDbEnvironment.openSecondaryDatabase(null, secDbName, myDB, secConfig);
    }

    private void setEnvironment()
    {
        envLocation = ConfigManager.getDbDir();
        envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setLocking(false);
        myDbEnvironment = new Environment(new File(envLocation),
                envConfig);
    }

    private void loadData(){
        ScanOperatorPlain scanOp = new ScanOperatorPlain(new Table("", tableName)); //check tableSchema
        HashMap<String,ColumnDetail> schema = scanOp.getOutputTupleSchema();
        ArrayList<Datum> tuple =  scanOp.readOneTuple();

        while(tuple != null)
        {
            DataRow row = new DataRow(tuple);
            //Collect stats here

            DatabaseEntry valueDBEntry = new DatabaseEntry();
            TupleBinding<DataRow> tupleBinder = new GenericTupleBinder(Main.indexTypeMaps.get(tableName));

            tupleBinder.objectToEntry(row, valueDBEntry);

            KeyClass key;
            DatabaseEntry keyDBEntry = new DatabaseEntry();
            TupleBinding<KeyClass> keyClassBinder = new KeyClassBinder();
			/*
			key = new KeyClass(generateKey(tblName, tuple, schema));
			//Doubtful if we need a Hash here from 2 columns from the same table whose combination is a primary key */
            try {
                long keyValue = ((LongValue) row.getRow().get(keyPositions.get(0)).getValue()).getValue();
                if (keyPositions.size() > 1){
                    long keyValue2 = ((LongValue) row.getRow().get(keyPositions.get(1)).getValue()).getValue();
                    keyValue = getPHash(keyValue, keyValue2);
                }
                key = new KeyClass(keyValue);
                keyClassBinder.objectToEntry(key, keyDBEntry);
                myDB.put(null, keyDBEntry, valueDBEntry);  //write to BDB
            }
            catch (ClassCastException cs){
                System.err.println("Could not parse keys " + keyPositions + " for " + tableName);
            }

            tuple = scanOp.readOneTuple();
        }
    }

    private void setConfig(DatabaseConfig mydbConfig, SecondaryConfig mySecDBConfig) {
        mydbConfig.setTransactional(false);
        mySecDBConfig.setTransactional(false);
    }
    private void closeEnvironment() {
        myDbEnvironment.close();
    }

    /* Cantar Pairing function get perfect hashes from two unique numbers
A = a >= 0 ? 2 * a : -2 * a - 1;
B = b >= 0 ? 2 * b : -2 * b - 1;
(A + B) * (A + B + 1) / 2 + A;
*/
    private long getPHash(long a ,long b){
        long A = 2*a;
        long B = 2*b;

        return ((A + B) * (A+B+1) / 2 + A);
    }
}
