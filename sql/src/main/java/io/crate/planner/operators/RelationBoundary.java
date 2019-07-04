/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.planner.operators;

import io.crate.analyze.OrderBy;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.common.collections.Lists2;
import io.crate.data.Row;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.expression.symbol.FieldsVisitor;
import io.crate.expression.symbol.RefVisitor;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.PlannerContext;
import io.crate.planner.SubqueryPlanner;
import io.crate.sql.tree.QualifiedName;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RelationBoundary extends ForwardingLogicalPlan {

    public static LogicalPlan.Builder create(LogicalPlan.Builder sourceBuilder,
                                             AnalyzedRelation relation,
                                             SubqueryPlanner subqueryPlanner) {
        return (tableStats, hints) -> {
            HashMap<Symbol, Symbol> reverseMapping = new HashMap<>();
            LogicalPlan source = sourceBuilder.build(tableStats, hints);
            for (Symbol symbol : source.outputs()) {
                RefVisitor.visitRefs(symbol, r -> {
                    ScopedSymbol field = new ScopedSymbol(relation, r);
                    reverseMapping.putIfAbsent(r, field);
                });
                FieldsVisitor.visitFields(symbol, f -> {
                    ScopedSymbol field = new ScopedSymbol(relation, f);
                    reverseMapping.putIfAbsent(f, field);
                });
            }
            List<Symbol> outputs = OperatorUtils.mappedSymbols(source.outputs(), reverseMapping);
            Map<LogicalPlan, SelectSymbol> subQueries = subqueryPlanner.planSubQueries(relation);
            return new RelationBoundary(source, relation, outputs, reverseMapping, subQueries);
        };
    }

    private final Map<LogicalPlan, SelectSymbol> dependencies;
    private final List<Symbol> outputs;

    private final AnalyzedRelation relation;
    private final Map<Symbol, Symbol> reverseMapping;

    public RelationBoundary(LogicalPlan source,
                            AnalyzedRelation relation,
                            List<Symbol> outputs,
                            Map<Symbol, Symbol> reverseMapping,
                            Map<LogicalPlan, SelectSymbol> subQueries) {
        super(source);
        this.outputs = outputs;
        Map<LogicalPlan, SelectSymbol> allSubQueries = new HashMap<>();
        allSubQueries.putAll(subQueries);
        allSubQueries.putAll(source.dependencies());
        this.dependencies = Collections.unmodifiableMap(allSubQueries);
        this.relation = relation;
        this.reverseMapping = reverseMapping;
    }

    @Override
    public Set<QualifiedName> getRelationNames() {
        return Set.of(relation.getQualifiedName());
    }

    @Override
    public ExecutionPlan build(PlannerContext plannerContext,
                               ProjectionBuilder projectionBuilder,
                               int limit,
                               int offset,
                               @Nullable OrderBy order,
                               @Nullable Integer pageSizeHint,
                               Row params,
                               SubQueryResults subQueryResults) {
        return source.build(
            plannerContext, projectionBuilder, limit, offset, order, pageSizeHint, params, subQueryResults);
    }

    @Override
    public List<Symbol> outputs() {
        return outputs;
    }

    @Override
    public LogicalPlan replaceSources(List<LogicalPlan> sources) {
        LogicalPlan newSource = Lists2.getOnlyElement(sources);
        return new RelationBoundary(
            newSource,
            relation,
            OperatorUtils.mappedSymbols(newSource.outputs(), reverseMapping),
            reverseMapping,
            dependencies
        );
    }

    @Override
    public Map<LogicalPlan, SelectSymbol> dependencies() {
        return dependencies;
    }

    @Override
    public String toString() {
        return "Boundary{" + source + '}';
    }

    @Override
    public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
        return visitor.visitRelationBoundary(this, context);
    }
}
