/*
 * Copyright 2010-2019 Boxfuse GmbH
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
import org.flywaydb.core.internal.jdbc.JdbcConnectionFactory;

/**
 * DB2 database.
 */
public class DB2Database extends Database<DB2Connection> {
    /**
     * Creates a new instance.
     *
     * @param configuration The Flyway configuration.
     */
    public DB2Database(Configuration configuration, JdbcConnectionFactory jdbcConnectionFactory



    ) {
        super(configuration, jdbcConnectionFactory



        );
    }

    @Override
    protected DB2Connection doGetConnection(Connection connection) {
        return new DB2Connection(this, connection);
    }











    @Override
    public final void ensureSupported() {
        ensureDatabaseIsRecentEnough("9.7");

        ensureDatabaseNotOlderThanOtherwiseRecommendUpgradeToFlywayEdition("11.1", org.flywaydb.core.internal.license.Edition.ENTERPRISE);

        recommendFlywayUpgradeIfNecessary("11.5");
    }

    @Override
    public String getRawCreateScript(Table table, boolean baseline) {
        String tablespace = configuration.getTablespace() == null
                ? ""
                : " IN \"" + configuration.getTablespace() + "\"";

        return "CREATE TABLE " + table + " (\n" +
                "    \"" + configuration.getInstalledRankColumn() + "\" INT NOT NULL,\n" +
                "    \"" + configuration.getVersionColumn() + "\" VARCHAR(50),\n" +
                "    \"" + configuration.getDescriptionColumn() + "\" VARCHAR(200) NOT NULL,\n" +
                "    \"" + configuration.getTypeColumn() + "\" VARCHAR(20) NOT NULL,\n" +
                "    \"" + configuration.getScriptColumn() + "\" VARCHAR(1000) NOT NULL,\n" +
                "    \"" + configuration.getChecksumColumn() + "\" INT,\n" +
                "    \"" + configuration.getInstalledByColumn() + "\" VARCHAR(100) NOT NULL,\n" +
                "    \"" + configuration.getInstalledOnColumn() + "\" TIMESTAMP DEFAULT CURRENT TIMESTAMP NOT NULL,\n" +
                "    \"" + configuration.getExecutionTimeColumn() + "\" INT NOT NULL,\n" +
                "    \"" + configuration.getSuccessColumn() + "\" SMALLINT NOT NULL,\n" +
                "    CONSTRAINT \"" + table.getName() + "_s\" CHECK (\"success\" in(0,1))\n" +
                ")" +



                        " ORGANIZE BY ROW"



                + tablespace + ";\n" +
                "ALTER TABLE " + table + " ADD CONSTRAINT \"" + table.getName() + "_pk\" PRIMARY KEY (\"" + configuration.getInstalledRankColumn() + "\");\n" +
                "CREATE INDEX \"" + table.getSchema().getName() + "\".\"" + table.getName() + "_s_idx\" ON " + table + " (\"success\");" +
                (baseline ? getBaselineStatement(table) + ";\n" : "");
    }

    @Override
    public String getSelectStatement(Table table) {
        return super.getSelectStatement(table)
                // Allow uncommitted reads so info can be invoked while migrate is running
                + " WITH UR";
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