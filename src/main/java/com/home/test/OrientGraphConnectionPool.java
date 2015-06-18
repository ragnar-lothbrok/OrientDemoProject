package com.home.test;

import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;

public class OrientGraphConnectionPool {

    private static OrientGraphConnectionPool orientGraphServiceFactory = null;

    private static OrientGraphFactory orientGraphFactory = null;

    private static String DB_PATH = "remote:localhost/AccessControl";

    private static final Integer MIN_POOL_SIZE = 1;
    private static final Integer MAX_POOL_SIZE = 10;

    private OrientGraphConnectionPool() {

    }

    public int getAvailablePoolSize() {
        if (orientGraphFactory != null) {
            return orientGraphFactory.getAvailableInstancesInPool();
        }
        return 0;
    }

    public int getTotalPoolSize() {
        if (orientGraphFactory != null) {
            return orientGraphFactory.getCreatedInstancesInPool();
        }
        return 0;
    }

    OrientBaseGraph getOrientGraph(boolean isTransactional) {
        if (orientGraphFactory == null) {
            synchronized (OrientGraphConnectionPool.class) {
                if (orientGraphFactory == null) {
                    orientGraphFactory = new OrientGraphFactory(DB_PATH, "admin", "admin");
                    orientGraphFactory.setupPool(MIN_POOL_SIZE, MAX_POOL_SIZE);
                    // Operations should be in transactions
                    orientGraphFactory.setRequireTransaction(true);
                    orientGraphFactory.setAutoStartTx(false);
                }
            }
        }
        return (orientGraphFactory == null ? null : (isTransactional ? orientGraphFactory.getTx() : orientGraphFactory
                .getNoTx()));
    }

    public static OrientGraphConnectionPool getInstance() {
        if (orientGraphServiceFactory == null) {
            synchronized (OrientGraphConnectionPool.class) {
                if (orientGraphServiceFactory == null) {
                    orientGraphServiceFactory = new OrientGraphConnectionPool();
                }
            }
        }
        return orientGraphServiceFactory;
    }

    public OrientGraphFactory getOrientGraphFactory() {
        if (orientGraphFactory == null) {
            synchronized (OrientGraphConnectionPool.class) {
                if (orientGraphFactory == null) {
                    orientGraphFactory = new OrientGraphFactory(DB_PATH, "admin", "admin");
                    orientGraphFactory.setupPool(MIN_POOL_SIZE, MAX_POOL_SIZE);
                    // Operations should be in transactions
                    orientGraphFactory.setRequireTransaction(true);
                    orientGraphFactory.setAutoStartTx(false);
                }
            }
        }
        return orientGraphFactory;
    }
}
