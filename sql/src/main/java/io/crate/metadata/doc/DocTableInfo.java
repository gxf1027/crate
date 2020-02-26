/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.metadata.doc;

import io.crate.action.sql.SessionContext;
import io.crate.analyze.WhereClause;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.expression.symbol.AnalyzedCheckConstraint;
import io.crate.expression.symbol.DynamicReference;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.IndexReference;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.sys.TableColumn;
import io.crate.metadata.table.Operation;
import io.crate.metadata.table.ShardedTable;
import io.crate.metadata.table.StoredTable;
import io.crate.metadata.table.TableInfo;
import io.crate.sql.tree.ColumnPolicy;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;


/**
 * Represents a user table.
 * <p>
 *     A user table either maps to 1 lucene index (if not partitioned)
 *     Or to multiple indices (if partitioned, or an alias)
 * </p>
 *
 * <p>
 *     See the following table for examples how the indexName is encoded.
 *     Functions to encode/decode are in {@link io.crate.metadata.IndexParts}
 * </p>
 *
 * <table>
 *     <tr>
 *         <th>schema</th>
 *         <th>tableName</th>
 *         <th>indices</th>
 *         <th>partitioned</th>
 *         <th>templateName</th>
 *     </tr>
 *
 *     <tr>
 *         <td>doc</td>
 *         <td>t1</td>
 *         <td>[ t1 ]</td>
 *         <td>NO</td>
 *         <td></td>
 *     </tr>
 *     <tr>
 *         <td>doc</td>
 *         <td>t1p</td>
 *         <td>[ .partitioned.t1p.&lt;ident&gt; ]</td>
 *         <td>YES</td>
 *         <td>.partitioned.t1p.</td>
 *     </tr>
 *     <tr>
 *         <td>custom</td>
 *         <td>t1</td>
 *         <td>[ custom.t1 ]</td>
 *         <td>NO</td>
 *         <td></td>
 *     </tr>
 *     <tr>
 *         <td>custom</td>
 *         <td>t1p</td>
 *         <td>[ custom..partitioned.t1p.&lt;ident&gt; ]</td>
 *         <td>YES</td>
 *         <td>custom..partitioned.t1p.</td>
 *     </tr>
 * </table>
 *
 */
public class DocTableInfo implements TableInfo, ShardedTable, StoredTable {

    private final Collection<Reference> columns;
    private final List<GeneratedReference> generatedColumns;
    private final List<Reference> partitionedByColumns;
    private final List<Reference> defaultExpressionColumns;
    private final Collection<ColumnIdent> notNullColumns;
    private final Map<ColumnIdent, IndexReference> indexColumns;
    private final Map<ColumnIdent, Reference> references;
    private final Map<ColumnIdent, String> analyzers;
    private final RelationName ident;
    private final List<ColumnIdent> primaryKeys;
    private final List<AnalyzedCheckConstraint> checkConstraints;
    private final ColumnIdent clusteredBy;
    private final String[] concreteIndices;
    private final String[] concreteOpenIndices;
    private final List<ColumnIdent> partitionedBy;
    private final int numberOfShards;
    private final String numberOfReplicas;
    private final Map<String, Object> tableParameters;
    private final TableColumn docColumn;
    private final Set<Operation> supportedOperations;

    private final List<PartitionName> partitions;

    private final boolean hasAutoGeneratedPrimaryKey;
    private final boolean isPartitioned;
    private final Version versionCreated;
    private final Version versionUpgraded;

    private final boolean closed;

    private final ColumnPolicy columnPolicy;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    public DocTableInfo(RelationName ident,
                        Collection<Reference> columns,
                        List<Reference> partitionedByColumns,
                        List<GeneratedReference> generatedColumns,
                        Collection<ColumnIdent> notNullColumns,
                        Map<ColumnIdent, IndexReference> indexColumns,
                        Map<ColumnIdent, Reference> references,
                        Map<ColumnIdent, String> analyzers,
                        List<ColumnIdent> primaryKeys,
                        List<AnalyzedCheckConstraint> checkConstraints,
                        ColumnIdent clusteredBy,
                        boolean hasAutoGeneratedPrimaryKey,
                        String[] concreteIndices,
                        String[] concreteOpenIndices,
                        IndexNameExpressionResolver indexNameExpressionResolver,
                        int numberOfShards,
                        String numberOfReplicas,
                        Map<String, Object> tableParameters,
                        List<ColumnIdent> partitionedBy,
                        List<PartitionName> partitions,
                        ColumnPolicy columnPolicy,
                        @Nullable Version versionCreated,
                        @Nullable Version versionUpgraded,
                        boolean closed,
                        Set<Operation> supportedOperations) {
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        assert (partitionedBy.size() ==
                partitionedByColumns.size()) : "partitionedBy and partitionedByColumns must have same amount of items in list";
        this.columns = columns;
        this.partitionedByColumns = partitionedByColumns;
        this.generatedColumns = generatedColumns;
        this.notNullColumns = notNullColumns;
        this.indexColumns = indexColumns;
        this.references = references;
        this.analyzers = analyzers;
        this.ident = ident;
        this.primaryKeys = primaryKeys;
        this.checkConstraints = checkConstraints;
        this.clusteredBy = clusteredBy;
        this.concreteIndices = concreteIndices;
        this.concreteOpenIndices = concreteOpenIndices;
        this.numberOfShards = numberOfShards;
        this.numberOfReplicas = numberOfReplicas;
        this.tableParameters = tableParameters;
        this.hasAutoGeneratedPrimaryKey = hasAutoGeneratedPrimaryKey;
        isPartitioned = !partitionedByColumns.isEmpty();
        this.partitionedBy = partitionedBy;
        this.partitions = partitions;
        this.columnPolicy = columnPolicy;
        this.versionCreated = versionCreated;
        this.versionUpgraded = versionUpgraded;
        this.closed = closed;
        this.supportedOperations = supportedOperations;
        // scale the fetchrouting timeout by n# of partitions
        this.docColumn = new TableColumn(DocSysColumns.DOC, references);
        this.defaultExpressionColumns = references.values()
            .stream()
            .filter(r -> r.defaultExpression() != null)
            .collect(Collectors.toList());
    }

    @Nullable
    public Reference getReference(ColumnIdent columnIdent) {
        Reference reference = references.get(columnIdent);
        if (reference == null) {
            return docColumn.getReference(ident(), columnIdent);
        }
        return reference;
    }


    @Override
    public Collection<Reference> columns() {
        return columns;
    }

    public List<Reference> defaultExpressionColumns() {
        return defaultExpressionColumns;
    }

    public List<GeneratedReference> generatedColumns() {
        return generatedColumns;
    }

    @Override
    public RowGranularity rowGranularity() {
        return RowGranularity.DOC;
    }

    @Override
    public RelationName ident() {
        return ident;
    }

    @Override
    public Routing getRouting(ClusterState state,
                              RoutingProvider routingProvider,
                              final WhereClause whereClause,
                              RoutingProvider.ShardSelection shardSelection,
                              SessionContext sessionContext) {
        String[] indices;
        if (whereClause.partitions().isEmpty()) {
            indices = concreteOpenIndices;
        } else {
            indices = whereClause.partitions().toArray(new String[0]);
        }
        Map<String, Set<String>> routingMap = null;
        if (whereClause.clusteredBy().isEmpty() == false) {
            Set<String> routing = whereClause.routingValues();
            if (routing == null) {
                routing = Collections.emptySet();
            }
            routingMap = indexNameExpressionResolver.resolveSearchRouting(
                state, routing, indices);
        }

        if (routingMap == null) {
            routingMap = Collections.emptyMap();
        }
        return routingProvider.forIndices(state, indices, routingMap, isPartitioned, shardSelection);
    }

    public List<ColumnIdent> primaryKey() {
        return primaryKeys;
    }

    @Override
    public List<AnalyzedCheckConstraint> checkConstraints() {
        return checkConstraints;
    }

    @Override
    public int numberOfShards() {
        return numberOfShards;
    }

    @Override
    public String numberOfReplicas() {
        return numberOfReplicas;
    }

    @Override
    public ColumnIdent clusteredBy() {
        return clusteredBy;
    }

    public boolean hasAutoGeneratedPrimaryKey() {
        return hasAutoGeneratedPrimaryKey;
    }

    public String[] concreteIndices() {
        return concreteIndices;
    }

    public String[] concreteOpenIndices() {
        return concreteOpenIndices;
    }

    /**
     * columns this table is partitioned by.
     * <p>
     * guaranteed to be in the same order as defined in CREATE TABLE statement
     *
     * @return always a list, never null
     */
    public List<Reference> partitionedByColumns() {
        return partitionedByColumns;
    }

    /**
     * column names of columns this table is partitioned by (in dotted syntax).
     * <p>
     * guaranteed to be in the same order as defined in CREATE TABLE statement
     *
     * @return always a list, never null
     */
    public List<ColumnIdent> partitionedBy() {
        return partitionedBy;
    }

    public List<PartitionName> partitions() {
        return partitions;
    }

    /**
     * returns <code>true</code> if this table is a partitioned table,
     * <code>false</code> otherwise
     * <p>
     * if so, {@linkplain #partitions()} returns infos about the concrete indices that make
     * up this virtual partitioned table
     */
    public boolean isPartitioned() {
        return isPartitioned;
    }

    public IndexReference indexColumn(ColumnIdent ident) {
        return indexColumns.get(ident);
    }

    public Iterator<IndexReference> indexColumns() {
        return indexColumns.values().iterator();
    }

    @Override
    public Iterator<Reference> iterator() {
        return references.values().iterator();
    }

    /**
     * return the column policy of this table
     * that defines how adding new columns will be handled.
     * <ul>
     * <li><code>STRICT</code> means no new columns are allowed
     * <li><code>DYNAMIC</code> means new columns will be added to the schema
     * <li><code>IGNORED</code> means new columns will not be added to the schema.
     * those ignored columns can only be selected.
     * </ul>
     */
    public ColumnPolicy columnPolicy() {
        return columnPolicy;
    }

    @Nullable
    @Override
    public Version versionCreated() {
        return versionCreated;
    }

    @Nullable
    @Override
    public Version versionUpgraded() {
        return versionUpgraded;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    public Map<String, Object> parameters() {
        return tableParameters;
    }

    @Override
    public Set<Operation> supportedOperations() {
        return supportedOperations;
    }

    @Override
    public RelationType relationType() {
        return RelationType.BASE_TABLE;
    }

    public String getAnalyzerForColumnIdent(ColumnIdent ident) {
        return analyzers.get(ident);
    }

    @Nullable
    public DynamicReference getDynamic(ColumnIdent ident, boolean forWrite) {
        boolean parentIsIgnored = false;
        ColumnPolicy parentPolicy = columnPolicy();
        if (!ident.isTopLevel()) {
            // see if parent is strict object
            ColumnIdent parentIdent = ident.getParent();
            Reference parentInfo = null;

            while (parentIdent != null) {
                parentInfo = getReference(parentIdent);
                if (parentInfo != null) {
                    break;
                }
                parentIdent = parentIdent.getParent();
            }

            if (parentInfo != null) {
                parentPolicy = parentInfo.columnPolicy();
            }
        }

        switch (parentPolicy) {
            case DYNAMIC:
                if (!forWrite) return null;
                break;
            case STRICT:
                if (forWrite) throw new ColumnUnknownException(ident.sqlFqn(), ident());
                return null;
            case IGNORED:
                parentIsIgnored = true;
                break;
            default:
                break;
        }
        if (parentIsIgnored) {
            return new DynamicReference(new ReferenceIdent(ident(), ident), rowGranularity(), ColumnPolicy.IGNORED);
        }
        return new DynamicReference(new ReferenceIdent(ident(), ident), rowGranularity());
    }

    @Override
    public String toString() {
        return ident.fqn();
    }

    public Collection<ColumnIdent> notNullColumns() {
        return notNullColumns;
    }
}
