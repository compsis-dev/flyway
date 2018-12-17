/*
 * Copyright 2010-2018 Boxfuse GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.flywaydb.core.internal.database.db2;

import java.sql.Connection;
import java.sql.SQLException;

import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.database.base.Database;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.placeholder.PlaceholderReplacer;
import org.flywaydb.core.internal.resource.LoadableResource;
import org.flywaydb.core.internal.resource.StringResource;
import org.flywaydb.core.internal.sqlscript.SqlStatementBuilderFactory;

/**
 * DB2 database.
 */
public class DB2Database extends Database<DB2Connection> {
    /**
     * Creates a new instance.
     *
     * @param configuration The Flyway configuration.
     * @param connection    The connection to use.
     */
    public DB2Database(Configuration configuration, Connection connection, boolean originalAutoCommit



    ) {
        super(configuration, connection, originalAutoCommit



        );
    }

    @Override
    protected DB2Connection getConnection(Connection connection



    ) {
        return new DB2Connection(configuration, this, connection, originalAutoCommit



        );
    }

    @Override
    public final void ensureSupported() {
        ensureDatabaseIsRecentEnough("9.7");

        ensureDatabaseNotOlderThanOtherwiseRecommendUpgradeToFlywayEdition("10.5", org.flywaydb.core.internal.license.Edition.ENTERPRISE);


        ensureDatabaseNotOlderThanOtherwiseRecommendUpgradeToFlywayEdition("11.1", org.flywaydb.core.internal.license.Edition.PRO);

        recommendFlywayUpgradeIfNecessary("11.1");
    }

    @Override
    protected SqlStatementBuilderFactory createSqlStatementBuilderFactory(PlaceholderReplacer placeholderReplacer



    ) {
        return new DB2SqlStatementBuilderFactory(placeholderReplacer);
    }

    @Override
    public LoadableResource getRawCreateScript() {
        return new StringResource("CREATE TABLE \"${schema}\".\"${table}\" (\n" +
                "    \"${installedRankColumn}\" INT NOT NULL,\n" +
                "    \"${versionColumn}\" VARCHAR(50),\n" +
                "    \"${descriptionColumn}\" VARCHAR(200) NOT NULL,\n" +
                "    \"${typeColumn}\" VARCHAR(20) NOT NULL,\n" +
                "    \"${scriptColumn}\" VARCHAR(1000) NOT NULL,\n" +
                "    \"${checksumColumn}\" INT,\n" +
                "    \"${installedByColumn}\" VARCHAR(100) NOT NULL,\n" +
                "    \"${installedOnColumn}\" TIMESTAMP DEFAULT CURRENT TIMESTAMP NOT NULL,\n" +
                "    \"${executionTimeColumn}\" INT NOT NULL,\n" +
                "    \"${successColumn}\" SMALLINT NOT NULL,\n" +
                "    CONSTRAINT \"${table}_s\" CHECK (\"success\" in(0,1))\n" +



                        ") ORGANIZE BY ROW;\n"



                + "ALTER TABLE \"${schema}\".\"${table}\" ADD CONSTRAINT \"${table}_pk\" PRIMARY KEY (\"${installedRankColumn}\");\n" +
                "\n" +
                "CREATE INDEX \"${schema}\".\"${table}_s_idx\" ON \"${schema}\".\"${table}\" (\"success\");");
    }

    @Override
    public String getSelectStatement(Table table, int maxCachedInstalledRank) {
        return super.getSelectStatement(table, maxCachedInstalledRank)
                // Allow uncommitted reads so info can be invoked while migrate is running
                + " WITH UR";
    }

    @Override
    public String getDbName() {
        return "db2";
    }

    @Override
    protected String doGetCurrentUser() throws SQLException {
        return getMainConnection().getJdbcTemplate().queryForString("select CURRENT_USER from sysibm.sysdummy1");
    }

    @Override
    public boolean supportsDdlTransactions() {
        return true;
    }

    @Override
    public boolean supportsChangingCurrentSchema() {
        return true;
    }

    @Override
    public String getBooleanTrue() {
        return "1";
    }

    @Override
    public String getBooleanFalse() {
        return "0";
    }

    @Override
    public String doQuote(String identifier) {
        return "\"" + identifier + "\"";
    }

    @Override
    public boolean catalogIsSchema() {
        return false;
    }

    @Override
    public boolean useSingleConnection() {
        return false;
    }

}