// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.trees.plans.commands.insert;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.EnvFactory;
import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.ExternalObjectLog;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HiveExternalMetaCache;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.persist.EditLog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.thrift.THivePartitionUpdate;
import org.apache.doris.transaction.TransactionManager;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Optional;

class HiveInsertExecutorTest {

    @AfterEach
    void tearDown() {
        ConnectContext.remove();
    }

    @Test
    void testCommittedInsertPublishesRowCountFenceWhenPartitionRefreshFails() throws Exception {
        ConnectContext ctx = new ConnectContext();
        ctx.setThreadLocalInfo();

        long catalogId = 61L;
        HMSExternalCatalog catalog = Mockito.mock(HMSExternalCatalog.class);
        ExternalDatabase<?> db = Mockito.mock(ExternalDatabase.class);
        HMSExternalTable table = Mockito.mock(HMSExternalTable.class);
        Mockito.when(catalog.getId()).thenReturn(catalogId);
        Mockito.when(catalog.getName()).thenReturn("hms");
        Mockito.when(catalog.getTransactionManager()).thenReturn(Mockito.mock(TransactionManager.class));
        Mockito.when(db.getId()).thenReturn(62L);
        Mockito.when(db.getFullName()).thenReturn("db1");
        Mockito.when(table.getCatalog()).thenReturn(catalog);
        Mockito.when(table.getDatabase()).thenReturn(db);
        Mockito.when(table.getName()).thenReturn("tbl1");
        Mockito.when(table.getNameWithFullQualifiers()).thenReturn("hms.db1.tbl1");
        Mockito.when(table.isPartitionedTable()).thenReturn(true);

        Env env = Mockito.mock(Env.class);
        ExternalMetaCacheMgr cacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        HiveExternalMetaCache hiveCache = Mockito.mock(HiveExternalMetaCache.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cacheMgr);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Mockito.when(cacheMgr.hive(catalogId)).thenReturn(hiveCache);
        Mockito.doThrow(new IllegalStateException("partition cache failure"))
                .when(hiveCache).refreshAffectedPartitions(
                        Mockito.eq(table), Mockito.anyList(), Mockito.anyList(), Mockito.anyList());

        EnvFactory envFactory = Mockito.mock(EnvFactory.class);
        Mockito.when(envFactory.createCoordinator(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyLong()))
                .thenReturn(Mockito.mock(Coordinator.class));
        try (MockedStatic<EnvFactory> mockedEnvFactory = Mockito.mockStatic(EnvFactory.class);
                MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnvFactory.when(EnvFactory::getInstance).thenReturn(envFactory);
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);

            TestingHiveInsertExecutor executor = new TestingHiveInsertExecutor(
                    ctx, table, Mockito.mock(NereidsPlanner.class));
            executor.setPartitionUpdates(Collections.singletonList(new THivePartitionUpdate()));
            Assertions.assertDoesNotThrow(executor::runAfterCommit);
        }

        InOrder order = Mockito.inOrder(cacheMgr, hiveCache);
        order.verify(cacheMgr).invalidateRowCountCache(table);
        order.verify(cacheMgr).hive(catalogId);
        order.verify(hiveCache).refreshAffectedPartitions(
                Mockito.eq(table), Mockito.anyList(), Mockito.anyList(), Mockito.anyList());
        Mockito.verify(cacheMgr).invalidateTableCache(table);

        ArgumentCaptor<ExternalObjectLog> logCaptor = ArgumentCaptor.forClass(ExternalObjectLog.class);
        Mockito.verify(editLog).logRefreshExternalTable(logCaptor.capture());
        ExternalObjectLog log = logCaptor.getValue();
        Assertions.assertEquals(catalogId, log.getCatalogId());
        Assertions.assertEquals("db1", log.getDbName());
        Assertions.assertEquals("tbl1", log.getTableName());
        Assertions.assertNull(log.getPartitionNames());
        Assertions.assertNull(log.getNewPartitionNames());
    }

    private static class TestingHiveInsertExecutor extends HiveInsertExecutor {
        TestingHiveInsertExecutor(ConnectContext ctx, HMSExternalTable table, NereidsPlanner planner) {
            super(ctx, table, "label", planner, Optional.empty(), false, 0L);
        }

        void setPartitionUpdates(java.util.List<THivePartitionUpdate> updates) throws Exception {
            Field field = HiveInsertExecutor.class.getDeclaredField("partitionUpdates");
            field.setAccessible(true);
            field.set(this, updates);
        }

        void runAfterCommit() throws DdlException {
            doAfterCommit();
        }
    }
}
