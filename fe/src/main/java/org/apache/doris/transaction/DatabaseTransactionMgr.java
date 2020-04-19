package org.apache.doris.transaction;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.persist.EditLog;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.ClearTransactionTask;
import org.apache.doris.task.PublishVersionTask;
import org.apache.doris.thrift.TTaskType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DatabaseTransactionMgr {

    private static final Logger LOG = LogManager.getLogger(DatabaseTransactionMgr.class);

    // the lock is used to control the access to transaction states
    // no other locks should be inside this lock
    private ReentrantReadWriteLock transactionLock = new ReentrantReadWriteLock(true);

    // transactionId -> TransactionState
    private Map<Long, TransactionState> idToTransactionState = Maps.newConcurrentMap();

    // to store transtactionStates with final status
    private ArrayDeque<TransactionState> finalStatusTransactionStateDeque = new ArrayDeque<>();

    // label -> txn ids
    // this is used for checking if label already used. a label may correspond to multiple txns,
    // and only one is success.
    // this member should be consistent with idToTransactionState, which means if a txn exist in idToTransactionState,
    // it must exists in dbIdToTxnLabels, and vice versa
    private Map<String, Set<Long>> labelToTxnIds = Maps.newConcurrentMap();


    // count the number of running txns of database, except for the routine load txn
    private AtomicInteger runningTxnNums = new AtomicInteger(0);

    // count only the number of running routine load txns of database
    private AtomicInteger runningRoutineLoadTxnNums = new AtomicInteger(0);

    private EditLog editLog;

    private List<ClearTransactionTask> clearTransactionTasks = Lists.newArrayList();

    void readLock() {
        this.transactionLock.readLock().lock();
    }

    void readUnlock() {
        this.transactionLock.readLock().unlock();
    }

    void writeLock() {
        this.transactionLock.writeLock().lock();
    }

    void writeUnlock() {
        this.transactionLock.writeLock().unlock();
    }

    public DatabaseTransactionMgr(EditLog editLog) {
        this.editLog = editLog;
    }

    public TransactionState getTransactionState(Long transactionId) {
        return idToTransactionState.get(transactionId);
    }

    public Set<Long> getTxnIdsByLabel(String label) {
        return labelToTxnIds.get(label);
    }

    public int getRunningTxnNums() {
        return runningTxnNums.get();
    }

    public int getRunningRoutineLoadTxnNums() {
        return runningRoutineLoadTxnNums.get();
    }

    public int getFinishedTxnNums() {
        return finalStatusTransactionStateDeque.size();
    }

    public void deleteTransactionState(TransactionState transactionState) {
        writeLock();
        try {
            if (!finalStatusTransactionStateDeque.isEmpty() &&
            transactionState.getTransactionId() == finalStatusTransactionStateDeque.getFirst().getTransactionId()) {
                finalStatusTransactionStateDeque.pop();
                Set<Long> txnIds = labelToTxnIds.get(transactionState.getLabel());
                txnIds.remove(transactionState.getTransactionId());
                if (txnIds.isEmpty()) {
                    labelToTxnIds.remove(transactionState.getDbId(), transactionState.getLabel());
                }
            }
        } finally {
            writeUnlock();
        }
    }

    public Map<Long, TransactionState> getIdToTransactionState() {
        return idToTransactionState;
    }

    void  unprotectedCommitTransaction(TransactionState transactionState, Set<Long> errorReplicaIds,
                                               Map<Long, Set<Long>> tableToPartition, Set<Long> totalInvolvedBackends,
                                               Database db) {
        // transaction state is modified during check if the transaction could committed
        if (transactionState.getTransactionStatus() != TransactionStatus.PREPARE) {
            return;
        }
        // update transaction state version
        transactionState.setCommitTime(System.currentTimeMillis());
        transactionState.setTransactionStatus(TransactionStatus.COMMITTED);
        transactionState.setErrorReplicas(errorReplicaIds);
        for (long tableId : tableToPartition.keySet()) {
            TableCommitInfo tableCommitInfo = new TableCommitInfo(tableId);
            for (long partitionId : tableToPartition.get(tableId)) {
                OlapTable table = (OlapTable) db.getTable(tableId);
                Partition partition = table.getPartition(partitionId);
                PartitionCommitInfo partitionCommitInfo = new PartitionCommitInfo(partitionId,
                        partition.getNextVersion(),
                        partition.getNextVersionHash());
                tableCommitInfo.addPartitionCommitInfo(partitionCommitInfo);
            }
            transactionState.putIdToTableCommitInfo(tableId, tableCommitInfo);
        }
        // persist transactionState
        unprotectUpsertTransactionState(transactionState);

        // add publish version tasks. set task to null as a placeholder.
        // tasks will be created when publishing version.
        for (long backendId : totalInvolvedBackends) {
            transactionState.addPublishVersionTask(backendId, null);
        }
    }

    // for add/update/delete TransactionState
    void unprotectUpsertTransactionState(TransactionState transactionState) {
        if (transactionState.getTransactionStatus() != TransactionStatus.PREPARE
                || transactionState.getSourceType() == TransactionState.LoadJobSourceType.FRONTEND) {
            // if this is a prepare txn, and load source type is not FRONTEND
            // no need to persist it. if prepare txn lost, the following commit will just be failed.
            // user only need to retry this txn.
            // The FRONTEND type txn is committed and running asynchronously, so we have to persist it.
            editLog.logInsertTransactionState(transactionState);
        }
        if (transactionState.getTransactionStatus().isFinalStatus()) {
            idToTransactionState.remove(transactionState.getTransactionId());
            finalStatusTransactionStateDeque.add(transactionState);
        } else {
            idToTransactionState.put(transactionState.getTransactionId(), transactionState);
        }

        updateTxnLabels(transactionState);
        updateDbRunningTxnNum(transactionState.getPreStatus(), transactionState);
    }

    private void updateTxnLabels(TransactionState transactionState) {
        Set<Long> txnIds = labelToTxnIds.get(transactionState.getLabel());
        if (txnIds == null) {
            txnIds = Sets.newHashSet();
            labelToTxnIds.put(transactionState.getLabel(), txnIds);
        }
        txnIds.add(transactionState.getTransactionId());
    }

    private void updateDbRunningTxnNum(TransactionStatus preStatus, TransactionState curTxnState) {
        AtomicInteger txnNum = null;
        if (curTxnState.getSourceType() == TransactionState.LoadJobSourceType.ROUTINE_LOAD_TASK) {
            txnNum = runningRoutineLoadTxnNums;
        } else {
            txnNum = runningTxnNums;
        }

        if (preStatus == null
                && (curTxnState.getTransactionStatus() == TransactionStatus.PREPARE
                || curTxnState.getTransactionStatus() == TransactionStatus.COMMITTED)) {
            txnNum.incrementAndGet();
        } else if ((preStatus == TransactionStatus.PREPARE
                || preStatus == TransactionStatus.COMMITTED)
                && (curTxnState.getTransactionStatus() == TransactionStatus.VISIBLE
                || curTxnState.getTransactionStatus() == TransactionStatus.ABORTED)) {
            txnNum.decrementAndGet();
        }
    }

    public void abortTransaction(long transactionId, String reason, TxnCommitAttachment txnCommitAttachment) throws UserException {
        if (transactionId < 0) {
            LOG.info("transaction id is {}, less than 0, maybe this is an old type load job, ignore abort operation", transactionId);
            return;
        }
        TransactionState transactionState = idToTransactionState.get(transactionId);
        if (transactionState == null) {
            throw new UserException("transaction not found");
        }

        // update transaction state extra if exists
        if (txnCommitAttachment != null) {
            transactionState.setTxnCommitAttachment(txnCommitAttachment);
        }

        // before state transform
        transactionState.beforeStateTransform(TransactionStatus.ABORTED);
        boolean txnOperated = false;
        writeLock();
        try {
            txnOperated = unprotectAbortTransaction(transactionId, reason);
        } finally {
            writeUnlock();
            transactionState.afterStateTransform(TransactionStatus.ABORTED, txnOperated, reason);
        }

        // send clear txn task to BE to clear the transactions on BE.
        // This is because parts of a txn may succeed in some BE, and these parts of txn should be cleared
        // explicitly, or it will be remained on BE forever
        // (However the report process will do the diff and send clear txn tasks to BE, but that is our
        // last defense)
        if (txnOperated && transactionState.getTransactionStatus() == TransactionStatus.ABORTED) {
            clearBackendTransactions(transactionState);
        }
    }

    private boolean unprotectAbortTransaction(long transactionId, String reason)
            throws UserException {
        TransactionState transactionState = idToTransactionState.get(transactionId);
        if (transactionState == null) {
            throw new UserException("transaction not found");
        }
        if (transactionState.getTransactionStatus() == TransactionStatus.ABORTED) {
            return false;
        }
        if (transactionState.getTransactionStatus() == TransactionStatus.COMMITTED
                || transactionState.getTransactionStatus() == TransactionStatus.VISIBLE) {
            throw new UserException("transaction's state is already "
                    + transactionState.getTransactionStatus() + ", could not abort");
        }
        transactionState.setFinishTime(System.currentTimeMillis());
        transactionState.setReason(reason);
        transactionState.setTransactionStatus(TransactionStatus.ABORTED);
        unprotectUpsertTransactionState(transactionState);
        for (PublishVersionTask task : transactionState.getPublishVersionTasks().values()) {
            AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.PUBLISH_VERSION, task.getSignature());
        }
        return true;
    }

    private void clearBackendTransactions(TransactionState transactionState) {
        Preconditions.checkState(transactionState.getTransactionStatus() == TransactionStatus.ABORTED);
        // for aborted transaction, we don't know which backends are involved, so we have to send clear task
        // to all backends.
        List<Long> allBeIds = Catalog.getCurrentSystemInfo().getBackendIds(false);
        AgentBatchTask batchTask = null;
        synchronized (clearTransactionTasks) {
            for (Long beId : allBeIds) {
                ClearTransactionTask task = new ClearTransactionTask(beId, transactionState.getTransactionId(), Lists.newArrayList());
                clearTransactionTasks.add(task);
            }

            // try to group send tasks, not sending every time a txn is aborted. to avoid too many task rpc.
            if (clearTransactionTasks.size() > allBeIds.size() * 2) {
                batchTask = new AgentBatchTask();
                for (ClearTransactionTask clearTransactionTask : clearTransactionTasks) {
                    batchTask.addTask(clearTransactionTask);
                }
                clearTransactionTasks.clear();
            }
        }

        if (batchTask != null) {
            AgentTaskExecutor.submit(batchTask);
        }
    }

    public void removeExpiredTxns() {
        long currentMillis = System.currentTimeMillis();
        writeLock();
        try {
            while (!finalStatusTransactionStateDeque.isEmpty()) {
                TransactionState transactionState = finalStatusTransactionStateDeque.getFirst();
                if (transactionState.isExpired(currentMillis)) {
                    finalStatusTransactionStateDeque.pop();
                    Set<Long> txnIds = labelToTxnIds.get(transactionState.getLabel());
                    txnIds.remove(transactionState.getTransactionId());
                    if (txnIds.isEmpty()) {
                        labelToTxnIds.remove(transactionState.getDbId(), transactionState.getLabel());
                    }
                    editLog.logDeleteTransactionState(transactionState);
                    LOG.info("transaction [" + transactionState.getTransactionId() + "] is expired, remove it from transaction manager");
                } else {
                    break;
                }

            }
        } finally {
            writeUnlock();
        }
    }

}
