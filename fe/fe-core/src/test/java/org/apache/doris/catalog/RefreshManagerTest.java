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

package org.apache.doris.catalog;

import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.ExternalObjectLog;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HiveExternalMetaCache;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Optional;

public class RefreshManagerTest {

    @Test
    void testColdDatabaseReplayInvalidatesCatalogRowCount() {
        long catalogId = 51L;
        ExternalCatalog catalog = Mockito.mock(ExternalCatalog.class);
        Mockito.when(catalog.getId()).thenReturn(catalogId);
        Mockito.when(catalog.getDbForReplay("db1")).thenReturn(Optional.empty());
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        Mockito.doReturn(catalog).when(catalogMgr).getCatalog(catalogId);
        ExternalMetaCacheMgr cacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cacheMgr);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            new RefreshManager().replayRefreshDb(ExternalObjectLog.createForRefreshDb(catalogId, "db1"));
        }

        Mockito.verify(cacheMgr).invalidateRowCountCache(catalogId);
    }

    @Test
    void testPartitionReplayInvalidatesRowCountBeforeCacheFailure() {
        long catalogId = 52L;
        HMSExternalCatalog catalog = Mockito.mock(HMSExternalCatalog.class);
        ExternalDatabase<?> db = Mockito.mock(ExternalDatabase.class);
        HMSExternalTable table = Mockito.mock(HMSExternalTable.class);
        Mockito.when(catalog.getId()).thenReturn(catalogId);
        Mockito.when(catalog.getDbForReplay("db1")).thenReturn(Optional.of(db));
        Mockito.doReturn(Optional.of(table)).when(db).getTableForReplay("tbl1");

        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        Mockito.doReturn(catalog).when(catalogMgr).getCatalog(catalogId);
        ExternalMetaCacheMgr cacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        HiveExternalMetaCache hiveCache = Mockito.mock(HiveExternalMetaCache.class);
        Mockito.when(cacheMgr.hive(catalogId)).thenReturn(hiveCache);
        Mockito.doThrow(new IllegalStateException("partition cache failure"))
                .when(hiveCache).refreshAffectedPartitionsCache(
                        Mockito.eq(table), Mockito.anyList(), Mockito.anyList());
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cacheMgr);

        ExternalObjectLog log = ExternalObjectLog.createForRefreshPartitions(
                catalogId, "db1", "tbl1",
                java.util.Collections.singletonList("p=1"), java.util.Collections.emptyList(), 1L);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertThrows(IllegalStateException.class,
                    () -> new RefreshManager().replayRefreshTable(log));
        }

        Mockito.verify(cacheMgr).invalidateRowCountCache(table);
    }

    @Test
    void testAlterPartitionInvalidatesRowCountBeforeCacheFailure() throws Exception {
        long catalogId = 53L;
        HMSExternalCatalog catalog = Mockito.mock(HMSExternalCatalog.class);
        ExternalDatabase<?> db = Mockito.mock(ExternalDatabase.class);
        HMSExternalTable table = Mockito.mock(HMSExternalTable.class);
        Mockito.when(catalog.getId()).thenReturn(catalogId);
        Mockito.doReturn(db).when(catalog).getDbNullable("db1");
        Mockito.when(db.getTableNullable("tbl1")).thenReturn(table);
        Mockito.when(table.getCatalog()).thenReturn(catalog);

        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        Mockito.doReturn(catalog).when(catalogMgr).getCatalog("hms");
        ExternalMetaCacheMgr cacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        Mockito.when(cacheMgr.hive(catalogId)).thenThrow(new IllegalStateException("partition cache failure"));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cacheMgr);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertThrows(IllegalStateException.class,
                    () -> new RefreshManager().refreshPartitions(
                            "hms", "db1", "tbl1", java.util.Collections.singletonList("p=1"), 1L, true));
        }

        Mockito.verify(cacheMgr).invalidateRowCountCache(table);
    }
}
