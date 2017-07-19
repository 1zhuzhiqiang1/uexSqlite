/*
 * Copyright (c) 2012-2016: Christopher J. Brody (aka Chris Brody)
 * Copyright (c) 2005-2010, Nitobi Software Inc.
 * Copyright (c) 2010, IBM Corporation
 */

package io.sqlc;

import android.content.Context;
import android.util.Log;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.zywx.wbpalmstar.engine.EBrowserView;
import org.zywx.wbpalmstar.engine.universalex.EUExBase;

import java.io.File;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class SQLitePlugin extends EUExBase {

    /**
     * Multiple database runner map (static).
     * NOTE: no public static accessor to db (runner) map since it would not work with db threading.
     * FUTURE put DBRunner into a public class that can provide external accessor.
     */
    static ConcurrentHashMap<String, DBRunner> dbrmap = new ConcurrentHashMap<String, DBRunner>();

    public SQLitePlugin(Context context, EBrowserView eBrowserView) {
        super(context, eBrowserView);
    }

    /**
     * 打印输出
     *
     * @param params [{value: 'test-string'}]
     */
    public void echoStringValue(String[] params) {
        int funcCallback = -1;
        if (params.length > 1) {
            funcCallback = Integer.parseInt(params[1]);
        }
        callbackToJs(funcCallback, false, params[0]);
    }

    /**
     * 打开数据库
     *
     * @param params [{name: 'demo.db', location: 'default'}]
     * @throws JSONException
     */
    public void open(String[] params) throws JSONException {
        int funcCallback = -1;
        if (params.length > 1) {
            funcCallback = Integer.parseInt(params[1]);
        }
        JSONArray args = new JSONArray(params[0]);
        JSONObject options = args.getJSONObject(0);
        String dbname = options.getString("name");
        // open database and start reading its queue
        this.startDatabase(dbname, options, funcCallback);
    }

    /**
     * 关闭数据库
     *
     * @param params [{path: this.dbname}]
     * @throws JSONException
     */
    public void close(String[] params) throws JSONException {
        int funcCallback = -1;
        if (params.length > 1) {
            funcCallback = Integer.parseInt(params[1]);
        }
        String dbname = params[0];
        // put request in the q to close the db
        this.closeDatabase(dbname, funcCallback);
    }

    /**
     * 删除数据库
     *
     * @param params [args]
     * @throws JSONException
     */
    public void delete(String[] params) throws JSONException {
        int funcCallback = -1;
        if (params.length > 1) {
            funcCallback = Integer.parseInt(params[1]);
        }
        JSONArray args = new JSONArray(params[0]);
        JSONObject o = args.getJSONObject(0);
        String dbname = o.getString("path");
        deleteDatabase(dbname, funcCallback);
    }

    /**
     * 执行sql语句
     *
     * @param params [{dbargs: {dbname: this.db.dbname},executes: tropts}] tropts:{sql:"",params:""}
     */
    public void executeSql(String[] params) {
        backgroundExecuteSqlBatch(params);
    }

    private void backgroundExecuteSqlBatch(String[] params) {
        int funcCallback = -1;
        if (params.length > 1) {
            funcCallback = Integer.parseInt(params[1]);
        }
        try {
            JSONArray tropts = new JSONArray(params[0]);
            JSONObject jsonObject = tropts.optJSONObject(0);
            JSONObject dbargs = jsonObject.optJSONObject("dbargs");
            String dbname = dbargs.optString("dbname");//数据库名称
            JSONArray txargs = jsonObject.optJSONArray("executes");
            if (txargs.isNull(0)) {
                callbackToJs(funcCallback, false, 1, "missing executes list");
            } else {
                int len = txargs.length();
                String[] queries = new String[len];
                JSONArray[] jsonparams = new JSONArray[len];
                for (int i = 0; i < len; i++) {
                    JSONObject a = txargs.optJSONObject(i);
                    queries[i] = a.optString("sql");
                    jsonparams[i] = a.optJSONArray("params");
                }
                // put db query in the queue to be executed in the db thread:
                DBQuery q = new DBQuery(queries, jsonparams);
                DBRunner r = dbrmap.get(dbname);
                if (r != null) {
                    try {
                        r.setFunCallback(funcCallback);
                        r.q.put(q);
                        new Thread(r).start();
                    } catch (Exception e) {
                        Log.e(SQLitePlugin.class.getSimpleName(), "couldn't add to queue", e);
                        callbackToJs(funcCallback, false, 1, "couldn't add to queue");
                    }
                } else {
                    callbackToJs(funcCallback, false, 1, "database not open");
                }
            }
        } catch (JSONException e) {
            e.printStackTrace();
        }
    }

    /**
     * Clean up and close all open databases.
     */
    @Override
    public void destroy() {
        super.destroy();
        while (!dbrmap.isEmpty()) {
            String dbname = dbrmap.keySet().iterator().next();
            this.closeDatabaseNow(dbname);
            DBRunner r = dbrmap.get(dbname);
            try {
                // stop the db runner thread:
                r.q.put(new DBQuery());
            } catch (Exception e) {
                Log.e(SQLitePlugin.class.getSimpleName(), "couldn't stop db thread", e);
            }
            dbrmap.remove(dbname);
        }
    }

    // --------------------------------------------------------------------------
    // LOCAL METHODS
    // --------------------------------------------------------------------------

    private void startDatabase(String dbname, JSONObject options, final int funCallback) {
        // TODO: is it an issue that we can orphan an existing thread?  What should we do here?
        // If we re-use the existing DBRunner it might be in the process of closing...
        DBRunner r = dbrmap.get(dbname);
        // Brody TODO: It may be better to terminate the existing db thread here & start a new one, instead.
        if (r != null) {
            // don't orphan the existing thread; just re-open the existing database.
            // In the worst case it might be in the process of closing, but even that's less serious
            // than orphaning the old DBRunner.
            callbackToJs(funCallback, false, 0, "打开数据库成功");
        } else {
            r = new DBRunner(dbname, options, funCallback, this);
            dbrmap.put(dbname, r);
//            this.cordova.getThreadPool().execute(r);
            // TODO: 17/7/14 应该使用线程池
            new Thread(r).start();
        }
    }

    /**
     * Open a database.
     *
     * @param dbname
     * @param old_impl
     * @return
     * @throws Exception
     */
    private SQLiteAndroidDatabase openDatabase(String dbname, boolean old_impl, final int funcCallback) throws Exception {
        try {
            // ASSUMPTION: no db (connection/handle) is already stored in the map
            // [should be true according to the code in DBRunner.run()]

            File dbfile = mContext.getDatabasePath(dbname);

            if (!dbfile.exists()) {
                dbfile.getParentFile().mkdirs();
            }

            Log.v("info", "Open sqlite db: " + dbfile.getAbsolutePath());

            SQLiteAndroidDatabase mydb = old_impl ? new SQLiteAndroidDatabase(this) : new SQLiteConnectorDatabase(this);
            mydb.open(dbfile);
//            callbackToJs(funcCallback, false, 0);
            return mydb;
        } catch (Exception e) {
            callbackToJs(funcCallback, false, 1, "can't open database " + e);
            throw e;
        }
    }

    /**
     * Close a database (in another thread).
     *
     * @param dbname
     */
    private void closeDatabase(String dbname, final int funcCallback) {
        DBRunner r = dbrmap.get(dbname);
        if (r != null) {
            SQLiteAndroidDatabase mydb = r.mydb;
            if (mydb != null) {
                mydb.closeDatabaseNow();
                dbrmap.remove(dbname);
                callbackToJs(funcCallback, false, 0, "Database close success");
            }
        }else{
            callbackToJs(funcCallback, false, 0, "Database close success");
        }
    }

    /**
     * Close a database (in the current thread).
     *
     * @param dbname The name of the database file
     */
    private void closeDatabaseNow(String dbname) {
        DBRunner r = dbrmap.get(dbname);

        if (r != null) {
            SQLiteAndroidDatabase mydb = r.mydb;
            if (mydb != null)
                mydb.closeDatabaseNow();
        }
    }

    private void deleteDatabase(String dbname, final int funCallback) {
        DBRunner r = dbrmap.get(dbname);
        if (r != null) {
            try {
                r.q.put(new DBQuery(true));
            } catch (Exception e) {
                callbackToJs(funCallback, false, 1, "couldn't close database" + e);
                Log.e(SQLitePlugin.class.getSimpleName(), "couldn't close database", e);
            }
        } else {
            boolean deleteResult = this.deleteDatabaseNow(dbname);
            if (deleteResult) {
                callbackToJs(funCallback, false, 0);
            } else {
                callbackToJs(funCallback, false, 1, "couldn't delete database");
            }
        }
    }

    /**
     * Delete a database
     *
     * @param dbname
     * @return
     */
    private boolean deleteDatabaseNow(String dbname) {
        File dbfile = mContext.getDatabasePath(dbname);
        try {
            return mContext.deleteDatabase(dbfile.getAbsolutePath());
        } catch (Exception e) {
            Log.e(SQLitePlugin.class.getSimpleName(), "couldn't delete database", e);
            return false;
        }
    }

    @Override
    protected boolean clean() {
        return false;
    }

    private class DBRunner implements Runnable {
        final String dbname;
        private boolean oldImpl;
        private boolean bugWorkaround;

        final BlockingQueue<DBQuery> q;
        SQLiteAndroidDatabase mydb;

        private int funCallback;
        private SQLitePlugin sqLitePlugin;

        DBRunner(final String dbname, JSONObject options, final int funCallback, final SQLitePlugin sqLitePlugin) {
            this.dbname = dbname;
            this.oldImpl = options.has("androidOldDatabaseImplementation");
            Log.v(SQLitePlugin.class.getSimpleName(), "Android db implementation: built-in android.database.sqlite package");
            this.bugWorkaround = this.oldImpl && options.has("androidBugWorkaround");
            if (this.bugWorkaround)
                Log.v(SQLitePlugin.class.getSimpleName(), "Android db closing/locking workaround applied");

            this.q = new LinkedBlockingQueue<DBQuery>();

            this.funCallback = funCallback;
            this.sqLitePlugin = sqLitePlugin;
        }

        public void setFunCallback(int funCallback) {
            this.funCallback = funCallback;
        }

        public void run() {
            try {
                this.mydb = openDatabase(dbname, this.oldImpl, funCallback);
            } catch (Exception e) {
                Log.e(SQLitePlugin.class.getSimpleName(), "unexpected error, stopping db thread", e);
                dbrmap.remove(dbname);
                return;
            }
            DBQuery dbq = null;
            try {
                dbq = q.take();
                while (!dbq.stop) {
                    mydb.executeSqlBatch(dbq.queries, dbq.jsonparams, funCallback);
                    if (this.bugWorkaround && dbq.queries.length == 1 && dbq.queries[0] == "COMMIT")
                        mydb.bugWorkaround();
                    dbq = q.take();
                }
            } catch (Exception e) {
                Log.e(SQLitePlugin.class.getSimpleName(), "unexpected error", e);
            }
            if (dbq != null && dbq.close) {
                try {
                    closeDatabaseNow(dbname);
                    dbrmap.remove(dbname); // (should) remove ourself
                    if (!dbq.delete) {
                        sqLitePlugin.callbackToJs(funCallback, false, 0);
                    } else {
                        try {
                            boolean deleteResult = deleteDatabaseNow(dbname);
                            if (deleteResult) {
                                sqLitePlugin.callbackToJs(funCallback, false, 0);
                            } else {
                                sqLitePlugin.callbackToJs(funCallback, false, 1, "couldn't delete database");
                            }
                        } catch (Exception e) {
                            Log.e(SQLitePlugin.class.getSimpleName(), "couldn't delete database", e);
                            sqLitePlugin.callbackToJs(funCallback, false, 1, "couldn't delete database: " + e);
                        }
                    }
                } catch (Exception e) {
                    Log.e(SQLitePlugin.class.getSimpleName(), "couldn't close database", e);
                    sqLitePlugin.callbackToJs(funCallback, false, 1, "couldn't close database: " + e);
                }
            }
        }
    }

    private final class DBQuery {
        // XXX TODO replace with DBRunner action enum:
        final boolean stop;
        final boolean close;
        final boolean delete;
        final String[] queries;
        final JSONArray[] jsonparams;

        DBQuery(String[] myqueries, JSONArray[] params) {
            this.stop = false;
            this.close = false;
            this.delete = false;
            this.queries = myqueries;
            this.jsonparams = params;
        }

        DBQuery(boolean delete) {
            this.stop = true;
            this.close = true;
            this.delete = delete;
            this.queries = null;
            this.jsonparams = null;
        }

        // signal the DBRunner thread to stop:
        DBQuery() {
            this.stop = true;
            this.close = false;
            this.delete = false;
            this.queries = null;
            this.jsonparams = null;
        }
    }

    private static enum Action {
        echoStringValue,
        open,
        close,
        delete,
        executeSqlBatch,
        backgroundExecuteSqlBatch,
    }
}

/* vim: set expandtab : */
