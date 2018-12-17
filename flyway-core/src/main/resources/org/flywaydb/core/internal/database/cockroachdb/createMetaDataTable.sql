--
-- Copyright 2010-2018 Boxfuse GmbH
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--         http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--

CREATE TABLE "${schema}"."${table}" (
    "${installedRankColumn}" INT NOT NULL PRIMARY KEY,
    "${versionColumn}" VARCHAR(50),
    "${descriptionColumn}" VARCHAR(200) NOT NULL,
    "${typeColumn}" VARCHAR(20) NOT NULL,
    "${scriptColumn}" VARCHAR(1000) NOT NULL,
    "${checksumColumn}" INTEGER,
    "${installedByColumn}" VARCHAR(100) NOT NULL,
    "${installedOnColumn}" TIMESTAMP NOT NULL DEFAULT now(),
    "${executionTimeColumn}" INTEGER NOT NULL,
    "${successColumn}" BOOLEAN NOT NULL
);

CREATE INDEX "${table}_s_idx" ON "${schema}"."${table}" ("${successColumn}");
