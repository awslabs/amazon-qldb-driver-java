package software.amazon.qldb;

import com.amazon.ion.IonInt;
import com.amazon.ion.IonStruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.qldbsession.QldbSessionClient;

/*
CFR: This file will be removed after Dev complete.
It exists to thoroughly test multi-thread scenarios. All verifications are done manually.
If there are some tests missing, we should add them here
 */
public class MultiThreadIntegrationTest {
    private static final Logger logger = LoggerFactory.getLogger(QldbSession.class);

    private QldbDriver qldbDriverImpl;

    public static void main(String... args) throws InterruptedException {
        final MultiThreadIntegrationTest multiThreadIntegrationTest = new MultiThreadIntegrationTest();
        multiThreadIntegrationTest.setup();

        //tests to check behavior with different poolLimit and threads combination for simple Tx
        logger.info("Running test oneThreadOnePoolLimit");
        multiThreadIntegrationTest.oneThreadOnePoolLimit();

        logger.info("Running test threeThreadsThreePoolLimit");
        multiThreadIntegrationTest.threeThreadsThreePoolLimit();

        logger.info("Running test threeThreadsOnePoolLimit");
        multiThreadIntegrationTest.threeThreadsOnePoolLimit();

        logger.info("Running test threeThreadsOnePoolLimitAndTimeout");
        multiThreadIntegrationTest.threeThreadsOnePoolLimitAndTimeout();

        //Test OCC
        logger.info("Running test threeThreadsAndOCC");
        multiThreadIntegrationTest.threeThreadsAndOCC();

    }

    //Expected: all methods to go through on same main thread
    private void setup() throws InterruptedException {
        initPooledDriver(1);
        createTables();
        Thread.sleep(3000);
        insertRecords();
    }

    //The single main thread gets the session and executes. Nothing crazy.
    private void oneThreadOnePoolLimit() throws InterruptedException {
        initPooledDriver(1);
        readRecords();
    }

    /*
    All threads should go through without waiting or retry.
    Ideally, three sessions will be created since all the threads ask for a session
    almost at the same time but it is not mandatory. (In all the runs of this test on
    my local machine, three sessions were created consistently.
     */
    private void threeThreadsThreePoolLimit() throws InterruptedException {
        initPooledDriver(3);
        readMultiThread();
    }

    /* Each thread will get session sequentially.
    As the poolTimeout is high enough, there should not be an error while waiting for the session to be available
     */
    private void threeThreadsOnePoolLimit() throws InterruptedException {
        initPooledDriver(1);
        readMultiThread();
    }

    /*
       [IMPORTANT]
       With the poolTimeout to just 1 ms, only one thread should go through.
       The other two threads will try to acquire the session , but because it can wait for only 1ms,
       they will error out.

       This test case affirms two behaviors:
       1. The driver does not go into infinite loop while trying to execute the Transaction
       2. We don't create unbounded session pool
     */
    private void threeThreadsOnePoolLimitAndTimeout() throws InterruptedException {
        initPooledDriver(1);
        readMultiThread();
    }

    /*
    This test case ensures that OCCs are handled correctly.
    After this test Jack's age will be 15 more than the initial age(50, if script is used in order)
    During the course of the execution, there might be OCCs, but at the end,
    it all works out.
     */
    private void threeThreadsAndOCC() throws InterruptedException {
        initPooledDriver(3);
        //Just logging the current record
        readRecords();
        //Update the age in three threads
        updateMultiThread();
        //Log the age after updates to check that the current age is 15 more than initially logged
        readRecords();
    }

    private void initPooledDriver(int poolLimit) {
        qldbDriverImpl = QldbDriver.builder()
                                   .maxConcurrentTransactions(poolLimit)
                                   .ledger("people")
                                   .transactionRetryPolicy(RetryPolicy.builder().maxRetries(5).build())
                                   .sessionClientBuilder(QldbSessionClient.builder())
                                   .build();
    }

    private void createTables() {
        qldbDriverImpl.execute(txn -> {
            Result result = txn.execute("SELECT * FROM information_schema.user_tables WHERE name = 'People'");
            if (result.isEmpty()) {
                logger.info("Creating table: People");
                txn.execute("CREATE TABLE People");
            } else {
                logger.info("People table already exists");
            }

        });
    }

    private void insertRecords() {
        qldbDriverImpl.execute(txn -> {
            final String query = "INSERT INTO People << {'name': 'Jack', 'age':50} >>";
            logger.info("query " + query);
            txn.execute(query);
        });
    }

    private void readRecords() {
        final Result persons = qldbDriverImpl.execute(txn -> {
            final Result result = txn.execute("SELECT name, age FROM People");
            return result;
        });

        persons.iterator().forEachRemaining(row -> {
            final IonStruct ionStruct = (IonStruct) row;
            logger.info(ionStruct.toString());
        });
    }

    private void updateRecord() {
        final Result persons = qldbDriverImpl.execute(txn -> {
            final Result result = txn.execute("SELECT name, age FROM People");
            final IonStruct personStruct = (IonStruct) result.iterator().next();
            final int currentAge = ((IonInt) personStruct.get("age")).intValue();
            final int newAge = currentAge + 5;

            final Result updateResult = txn.execute("UPDATE People SET age =" + newAge + "  WHERE name='Jack'");
            return updateResult;
        });

        persons.iterator().forEachRemaining(row -> {
            final IonStruct ionStruct = (IonStruct) row;
            logger.info(ionStruct.toString());
        });
    }


    private void readMultiThread() throws InterruptedException {
        final Thread t1 = new Thread() {
            @Override
            public void run() {
                logger.info("Starting Thread 1 For Read");
                readRecords();
            }
        };

        final Thread t2 = new Thread() {
            @Override
            public void run() {
                logger.info("Starting Thread 2 For Read");
                readRecords();
            }
        };

        final Thread t3 = new Thread() {
            @Override
            public void run() {
                logger.info("Starting Thread 3 For read");
                readRecords();
            }
        };

        t1.start();
        t2.start();
        t3.start();

        t1.join();
        t2.join();
        t3.join();
    }

    private void updateMultiThread() throws InterruptedException {
        final Thread t1 = new Thread() {
            @Override
            public void run() {
                logger.info("Starting Thread 1 For Update");
                updateRecord();
            }
        };

        final Thread t2 = new Thread() {
            @Override
            public void run() {
                logger.info("Starting Thread 2 For Update");
                updateRecord();
            }
        };

        final Thread t3 = new Thread() {
            @Override
            public void run() {
                logger.info("Starting Thread 3 For Update");
                updateRecord();
            }
        };

        t1.start();
        t2.start();
        t3.start();

        t1.join();
        t2.join();
        t3.join();

    }


}
