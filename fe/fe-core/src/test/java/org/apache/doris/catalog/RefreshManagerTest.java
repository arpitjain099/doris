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
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.ExternalObjectLog;

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
}
