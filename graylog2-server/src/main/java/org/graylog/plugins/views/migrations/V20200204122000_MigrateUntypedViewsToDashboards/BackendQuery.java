/**
 * This file is part of Graylog.
 *
 * Graylog is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Graylog is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Graylog.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.graylog.plugins.views.migrations.V20200204122000_MigrateUntypedViewsToDashboards;

import com.google.common.collect.ImmutableMap;
import org.bson.Document;

public class BackendQuery extends Document {
    private static final String FIELD_QUERY_STRING = "query_string";

    public BackendQuery(String queryString) {
        super(ImmutableMap.of(
                "type", "elasticsearch",
                FIELD_QUERY_STRING, queryString
        ));
    }
}
