/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.pdbo;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.Threads.threadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;
import io.airlift.concurrent.SetThreadName;
import io.airlift.concurrent.ThreadPoolExecutorMBean;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;

import com.mysql.jdbc.Driver;

public class StepCalcManager implements Runnable
{
    private static final Logger log = Logger.get(StepCalcManager.class);
    public static final String PDBO_DATABASE = "route_schema";
    public static final String PDBO_TABLE = PDBO_DATABASE + ".pdbo_table";
    public static final String PDBO_ROUTE = PDBO_DATABASE + ".pdbo_route";
    public static final String PDBO_LOG = PDBO_DATABASE + ".pdbo_log";
    private final ExecutorService executor;
    private final ThreadPoolExecutorMBean executorMBean;
    private final PriorityBlockingQueue<PdboTable> pdboQueue;

    private static final AtomicLong NEXT_RUNNER_ID = new AtomicLong();
    private volatile boolean closed;
    private final String connectionUrl;
    private final Properties connectionProperties;
    private final Duration pdboRefreshInterval;
    private final Duration pdboCleanHistoryInterval;
    private final int pdboCalcThreads;
    private long pdboLastUpdateTime = 0L;

    private final Driver driver;

    public StepCalcManager(String connectionUrl,
            Properties connectionProperties,
            Duration pdboRefreshInterval,
            Duration pdboCleanHistoryInterval,
            int pdboCalcThreads) throws SQLException
    {
        this.executor = newCachedThreadPool(threadsNamed("step-calc-processor-%d"));
        this.executorMBean = new ThreadPoolExecutorMBean((ThreadPoolExecutor) executor);

        this.pdboQueue = new PriorityBlockingQueue<>(Runtime.getRuntime().availableProcessors() * 10);
        this.closed = false;
        this.connectionUrl = connectionUrl;
        this.connectionProperties = connectionProperties;
        this.pdboRefreshInterval = pdboRefreshInterval;
        this.pdboCleanHistoryInterval = pdboCleanHistoryInterval;
        this.pdboCalcThreads = pdboCalcThreads;
        this.driver = new Driver();
    }

    public void run()
    {
        while (!closed) {
            try {
                if (pdboLastUpdateTime == 0L) {
                    addTableInfo();
                }
                else {
                    Thread.sleep(pdboRefreshInterval.toMillis());
                    long curTime = System.currentTimeMillis();
                    addTableInfo();
                    log.debug("Load pdbo table spend time : " + (System.currentTimeMillis() - curTime) + " ms");
                }
                long curTime = System.currentTimeMillis();
                if ((curTime - pdboLastUpdateTime) >= pdboCleanHistoryInterval.toMillis()) {
                    cleanPdboHistoryLogs();
                    log.debug("clean pdbo history logs finish in " + (System.currentTimeMillis() - curTime) + " ms");
                }
                pdboLastUpdateTime = System.currentTimeMillis();
            }
            catch (Exception e) {
                log.error("Load pdbo table error : ", e.getMessage());
                pdboLastUpdateTime = System.currentTimeMillis();
            }
        }
    }

    public void addTableInfo()
    {
        List<PdboTable> tables = null;
        String sql = "SELECT CONNECTORID,PRESTO_SCHEMA,PRESTO_TABLE FROM " + PDBO_TABLE
                + " WHERE CALC_STEP_ENABLE = 'Y' GROUP BY CONNECTORID,PRESTO_SCHEMA,PRESTO_TABLE";
        QueryRunner runner = new QueryRunner();
        Connection conn = getConnection();
        try {
            tables = runner.query(conn, sql, new PdboTableResultHandler());
        }
        catch (SQLException e) {
            log.error(e, "SQL : " + sql + ",addTableInfo error %s", e.getMessage());
        }
        finally {
            DbUtils.closeQuietly(conn);
        }
        startSplit(tables);
    }

    private class PdboTableResultHandler implements ResultSetHandler<List<PdboTable>>
    {
        @Override
        public List<PdboTable> handle(ResultSet rs) throws SQLException
        {
            List<PdboTable> tables = new ArrayList<>();
            while (rs.next()) {
                tables.add(new PdboTable()
                .setConnectorId(rs.getString(1))
                .setSchemaName(rs.getString(2))
                .setTableName(rs.getString(3)));
            }
            return tables;
        }
    }

    public void cleanPdboHistoryLogs()
    {
        String sql = "DELETE FROM " + PDBO_LOG + " WHERE RECORDFLAG IN ('runhistory','calchistory')";
        Connection conn = getConnection();
        QueryRunner runner = new QueryRunner();
        try {
            runner.update(conn, sql);
        }
        catch (SQLException e) {
            log.error(e, "SQL : " + sql + ",cleanPdboHistoryLogs error %s", e.getMessage());
        }
        finally {
            DbUtils.closeQuietly(conn);
        }
    }

    private synchronized void startSplit(List<PdboTable> tables)
    {
        pdboQueue.addAll(tables);
    }

    @PostConstruct
    public synchronized void start()
    {
        checkState(!closed, "StepCalcManager is closed");
        for (int i = 0; i < pdboCalcThreads; i++) {
            addRunnerThread();
        }
    }

    @PreDestroy
    public synchronized void stop()
    {
        closed = true;
        executor.shutdownNow();
    }

    private synchronized void addRunnerThread()
    {
        try {
            executor.execute(new Runner());
        }
        catch (RejectedExecutionException ignored) {
        }
    }

    private class Runner implements Runnable
    {
        private final long runnerId = NEXT_RUNNER_ID.getAndIncrement();

        @Override
        public void run()
        {
            try (SetThreadName runnerName = new SetThreadName("SplitRunner-%s", runnerId)) {
                while (!closed && !Thread.currentThread().isInterrupted()) {
                    final PdboTable take;
                    try {
                        take = pdboQueue.take();
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    stepCalc(take);
                }
            }
            finally {
                if (!closed) {
                    addRunnerThread();
                }
            }
        }
    }

    public void stepCalc(PdboTable table)
    {
        List<PdboTable> tableLogs = getTableRunningLogs(table);
        if (tableLogs == null || tableLogs.size() == 0) {
            return;
        }
        long allRows = tableLogs.stream().mapToLong(PdboTable::getRows).sum();
        int scanNodes = tableLogs.size();
        if (tableLogs.get(0).getScanNodes() != scanNodes) {
            return;
        }
        long averageStep = allRows / scanNodes;
        List<PdboTable> result = new ArrayList<>();
        List<PdboTable> otherSplit = new ArrayList<>();
        int usedNodes = combineTableLog(tableLogs, averageStep, result, otherSplit);
        int unUsedNodes = scanNodes - usedNodes;
        long calcRows = otherSplit.stream().mapToLong(PdboTable::getRows).sum();
        for (PdboTable split : otherSplit) {
            long curNode = Math.round((split.getRows() * 1.0 / calcRows) * unUsedNodes);
            long targetChunkSize = (long) Math.ceil((split.getEndIndex() - split.getBeginIndex()) * 1.0 / curNode);
            long chunkOffset = split.getBeginIndex();
            while (chunkOffset < split.getEndIndex()) {
                long chunkLength = Math.min(targetChunkSize, split.getEndIndex() - chunkOffset);
                result.add(new PdboTable().setConnectorId(split.getConnectorId()).
                        setSchemaName(split.getSchemaName()).
                        setTableName(split.getTableName()).
                        setRows(split.getRows()).
                        setScanNodes(split.getScanNodes()).
                        setBeginIndex(chunkOffset).
                        setEndIndex(chunkOffset + chunkLength));
                chunkOffset += chunkLength;
            }
        }
        commitTableLogs(result, table);
    }

    private int combineTableLog(List<PdboTable> tableLogs, long averageStep,
            List<PdboTable> result, List<PdboTable> otherSplit)
    {
        List<PdboTable> tmp = new ArrayList<>();
        int usedNodes = 0;
        int logSize = 0;
        long sum = 0;
        for (PdboTable split : tableLogs) {
            boolean isCombine = split.getRows() < averageStep;
            logSize++;
            if (averageStep > (sum + split.getRows())) {
                tmp.add(split);
                sum += split.getRows();
                if (logSize == tableLogs.size()) {
                    combineResult(tmp, result);
                    usedNodes++;
                }
                continue;
            }
            else {
                if (!tmp.isEmpty()) {
                    combineResult(tmp, result);
                    usedNodes++;
                    tmp.clear();
                }
                if (isCombine) {
                    sum = split.getRows();
                    tmp.add(split);
                }
                else {
                    otherSplit.add(split);
                    sum = 0;
                }
            }
            if (logSize == tableLogs.size() && tmp.size() > 0) {
                combineResult(tmp, result);
                usedNodes++;
            }
        }
        return usedNodes;
    }

    private void combineResult(List<PdboTable> tmp, List<PdboTable> combineResult)
    {
        combineResult.add(new PdboTable().setConnectorId(tmp.get(0).getConnectorId()).
                setSchemaName(tmp.get(0).getSchemaName()).
                setTableName(tmp.get(0).getTableName()).
                setRows(tmp.stream().mapToLong(PdboTable::getRows).sum()).
                setScanNodes(tmp.get(0).getScanNodes()).
                setBeginIndex(tmp.get(0).getBeginIndex()).
                setEndIndex(tmp.get(tmp.size() - 1).getEndIndex()));
    }

    public List<PdboTable> getTableRunningLogs(PdboTable table)
    {
        List<PdboTable> tables = null;
        Connection conn = getConnection();
        QueryRunner runner = new QueryRunner();
        String sql = "SELECT CONNECTORID,SCHEMANAME,TABLENAME,ROWS,BEGININDEX,ENDINDEX,SCANNODES "
                + "FROM " + PDBO_LOG + " WHERE CONNECTORID = '" + table.getConnectorId() +
                "' AND SCHEMANAME = '" + table.getSchemaName() +
                "' AND TABLENAME = '" + table.getTableName() +
                "' and RECORDFLAG = 'new' ORDER BY BEGININDEX";
        try {
            tables = runner.query(conn, sql, new PdboLogResultHandler());
        }
        catch (SQLException e) {
            log.error(e, "SQL : " + sql + ",getTableRunningLogs error %s", e.getMessage());
        }
        finally {
            DbUtils.closeQuietly(conn);
        }
        return tables;
    }

    private class PdboLogResultHandler implements ResultSetHandler<List<PdboTable>>
    {
        @Override
        public List<PdboTable> handle(ResultSet rs) throws SQLException
        {
            List<PdboTable> tables = new ArrayList<>();
            while (rs.next()) {
                tables.add(new PdboTable().setConnectorId(rs.getString(1)).
                        setSchemaName(rs.getString(2)).
                        setTableName(rs.getString(3)).
                        setRows(rs.getLong(4)).
                        setBeginIndex(rs.getLong(5)).
                        setEndIndex(rs.getLong(6)).
                        setScanNodes(rs.getInt(7)));
            }
            return tables;
        }
    }

    public void commitTableLogs(List<PdboTable> result, PdboTable pdboTable)
    {
        long timeStamp = System.nanoTime();
        int scanNodes = result.get(0).getScanNodes();
        boolean shouldUpdateTableRoute = scanNodes != result.size();
        StringBuilder sql = new StringBuilder().append("INSERT INTO " + PDBO_LOG
                + "(CONNECTORID,SCHEMANAME,TABLENAME,ROWS,BEGININDEX,ENDINDEX,RECORDFLAG,SCANNODES,TIMESTAMP) VALUES");
        for (PdboTable table : result) {
            sql.append("('" + table.getConnectorId() + "',")
                .append("'" + table.getSchemaName() + "',")
                .append("'" + table.getTableName() + "',")
                .append(table.getRows() + ",")
                .append(table.getBeginIndex() + ",")
                .append(table.getEndIndex() + ",")
                .append("'finish',")
                .append(result.size() + ",")
                .append(timeStamp + "),");
        }
        String updateSql = "UPDATE " + PDBO_LOG + " SET RECORDFLAG = 'calchistory' "
                + "WHERE RECORDFLAG in ('new','finish') AND CONNECTORID = '" + pdboTable.getConnectorId() +
                "' AND SCHEMANAME = '" + pdboTable.getSchemaName() + "' AND TABLENAME = '" + pdboTable.getTableName() + "'";
        String updateTableRouteSql = "UPDATE " + PDBO_ROUTE + " SET SCANNODENUMBER = " + result.size() +
                " WHERE TABLEID IN (SELECT TABLEID FROM " + PDBO_TABLE +
                " WHERE CONNECTORID='" + pdboTable.getConnectorId() +
                "' AND PRESTO_SCHEMA='" + pdboTable.getSchemaName() +
                "' AND PRESTO_TABLE='" + pdboTable.getTableName() + "')";
        Connection conn = getConnection();
        QueryRunner runner = new QueryRunner();
        try {
            runner.update(conn, updateSql);
            runner.update(conn, sql.substring(0, sql.length() - 1).toString());
            if (shouldUpdateTableRoute) {
                runner.update(conn, updateTableRouteSql);
            }
        }
        catch (SQLException e) {
            log.error(e, "updateSql = %s, updateTableRouteSql = %s, getTableRunningLogs error %s", updateSql, updateTableRouteSql, e.getMessage());
        }
        finally {
            DbUtils.closeQuietly(conn);
        }
    }

    public Connection getConnection()
    {
        Connection conn = null;
        try {
            conn = driver.connect(connectionUrl, connectionProperties);
        }
        catch (SQLException e) {
            log.error(e, "Connect db error %s" + e.getMessage());
        }
        return conn;
    }

    public ThreadPoolExecutorMBean getProcessorExecutor()
    {
        return executorMBean;
    }
}
