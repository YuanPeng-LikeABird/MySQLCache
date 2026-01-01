package com.my.sqlcache;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Semaphore;

public class SQLCacheConnectorPool {

    private static int POOL_SIZE = 8;
    private int myPoolSize;

    private List<SQLCacheConnector> connectorList = new ArrayList<>();

    private Semaphore mySem;

    public SQLCacheConnectorPool() {
        this(POOL_SIZE);
    }

    public SQLCacheConnectorPool(int poolSize) {
        myPoolSize = poolSize;
    }

    public void startup(String serverIp, int port) throws IOException {
        mySem = new Semaphore(myPoolSize);
        for (int i = 0; i < myPoolSize; ++i) {
            SQLCacheConnector connector = new SQLCacheConnector();
            connector.connect(serverIp, port);
            connectorList.add(connector);
        }
    }

    public void shutDown() {
        try {
            for (int i = 0; i < myPoolSize; ++i) {
                connectorList.get(i).disconnect();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public SQLCacheConnector get() {
        try {
            mySem.acquire();
            synchronized(this) {
                for (int i = 0; i < myPoolSize; ++i) {
                    if (!connectorList.get(i).isBusy()) {
                        SQLCacheConnector connector =  connectorList.get(i);
                        connector.setBusy(true);
                        return connector;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    public void free(SQLCacheConnector connector) {
        boolean isFree = false;
        synchronized (this) {
            for (int i = 0; i < myPoolSize; ++i) {
                if (connectorList.get(i) == connector) {
                    connector.setBusy(false);
                    isFree = true;
                    break;
                }
            }
        }

        if (isFree) {
            mySem.release();
        }
    }
}
