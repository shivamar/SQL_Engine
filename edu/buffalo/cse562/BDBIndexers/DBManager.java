package edu.buffalo.cse562.BDBIndexers;

import com.sleepycat.je.*;
import edu.buffalo.cse562.DTO.ConfigManager;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by keno on 5/2/15.
 */
public class DBManager {
    List<Cursor> cursors = new ArrayList<>();
    List<Database> databases = new ArrayList<>();
    List<SecondaryDatabase> secondaryDatabases = new ArrayList<>();
    List<SecondaryCursor> secondaryCursors = new ArrayList<>();
    List<DiskOrderedCursor> diskOrderedCursors = new ArrayList<>();
    private static DBManager thisInstance;
    private Environment myEnv;
    List<BufferedReader> bReaders = new ArrayList<>();

    public static DBManager getInstance() {
        if (thisInstance == null){
            thisInstance = new DBManager();
        }
        return thisInstance;
    }

    private Environment getEnvironment() {
        if (myEnv == null) {
            EnvironmentConfig envConfig = new EnvironmentConfig();
            envConfig.setCachePercent(70);
            envConfig.setLocking(false);
            envConfig.setTransactional(false);
            myEnv = new Environment(new File(ConfigManager.getDbDir()), envConfig);
        }
        return myEnv;
    }

    public Cursor getCursor(String tableName) {
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setReadOnly(true);
        dbConfig.setTransactional(false);
        Database db = getEnvironment().openDatabase(null, tableName, dbConfig);
        databases.add(db);
        Cursor cursor = db.openCursor(null, null);
        cursors.add(cursor);
        return cursor;
    }

    public DiskOrderedCursor getDiskOrderedCursor(String tableName) {
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setReadOnly(true);
        dbConfig.setTransactional(false);
        Database db = getEnvironment().openDatabase(null, tableName, dbConfig);
        databases.add(db);
        DiskOrderedCursorConfig docc = new DiskOrderedCursorConfig();
        docc.setQueueSize(5000);
        DiskOrderedCursor diskOrderedCursor = db.openCursor(docc);
        diskOrderedCursors.add(diskOrderedCursor);
        return diskOrderedCursor;
    }

    public SecondaryCursor getSecCursor(SecondaryConfig secConfig, String tableName) {
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setReadOnly(true);
        dbConfig.setTransactional(false);
        Database db = getEnvironment().openDatabase(null, tableName, dbConfig);
        databases.add(db);
        secConfig.setReadOnly(true);
        secConfig.setTransactional(false);
        secConfig.setSortedDuplicates(true);
        SecondaryDatabase secDb = getEnvironment().openSecondaryDatabase(null, tableName + "-secDB", db, secConfig);
        secondaryDatabases.add(secDb);
        SecondaryCursor secCursor = secDb.openCursor(null, null);
        secondaryCursors.add(secCursor);
        return secCursor;
    }

    public BufferedReader getBuffer(String tableName){
        //For direct scans
        try {
            Charset charset = Charset.forName("US-ASCII");
            Path dataFile = FileSystems.getDefault().getPath(ConfigManager.getDataDir(), tableName.toLowerCase() + ".dat");
            BufferedReader bufferedReader = Files.newBufferedReader(dataFile, charset);
            bReaders.add(bufferedReader);
            return bufferedReader;
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
    public void closeAll(){

        for (DiskOrderedCursor dc : diskOrderedCursors) {
            if (dc != null) {
                dc.close();
            }
        }

        for (Cursor c : cursors) {
            if (c != null) {
                c.close();
            }
        }

        for (SecondaryCursor sc : secondaryCursors) {
            if (sc != null) {
                sc.close();
            }
        }

        for (SecondaryDatabase sdb : secondaryDatabases) {
            if (sdb != null) {
                sdb.close();
            }
        }

        for (Database db : databases) {
            if (db != null) {
                db.close();
            }
        }
        if (myEnv != null) {
            myEnv.close();
        }

        for (BufferedReader b : bReaders){
            try {
                b.close();
            } catch (IOException e) {
            }
        }
    }

}
