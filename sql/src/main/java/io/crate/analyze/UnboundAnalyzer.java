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

package io.crate.analyze;

import io.crate.action.sql.SessionContext;
import io.crate.analyze.relations.RelationAnalyzer;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.sql.tree.AstVisitor;
import io.crate.sql.tree.BeginStatement;
import io.crate.sql.tree.Delete;
import io.crate.sql.tree.Explain;
import io.crate.sql.tree.InsertFromSubquery;
import io.crate.sql.tree.InsertFromValues;
import io.crate.sql.tree.Query;
import io.crate.sql.tree.ShowColumns;
import io.crate.sql.tree.ShowCreateTable;
import io.crate.sql.tree.ShowSchemas;
import io.crate.sql.tree.ShowSessionParameter;
import io.crate.sql.tree.ShowTables;
import io.crate.sql.tree.ShowTransaction;
import io.crate.sql.tree.Statement;
import io.crate.sql.tree.Update;

/**
 * Analyzer that can analyze statements without having access to the Parameters/ParameterContext.
 * <p>
 *     Currently this only works for select statements and the analysis cannot be re-used because it contains mutable components
 *     (like QuerySpec & the symbols within)
 * </p>
 */
class UnboundAnalyzer {

    private final UnboundDispatcher dispatcher;

    UnboundAnalyzer(RelationAnalyzer relationAnalyzer,
                    ShowCreateTableAnalyzer showCreateTableAnalyzer,
                    ShowStatementAnalyzer showStatementAnalyzer,
                    DeleteAnalyzer deleteAnalyzer,
                    UpdateAnalyzer updateAnalyzer,
                    InsertFromValuesAnalyzer insertFromValuesAnalyzer,
                    InsertFromSubQueryAnalyzer insertFromSubQueryAnalyzer,
                    ExplainStatementAnalyzer explainStatementAnalyzer) {
        this.dispatcher = new UnboundDispatcher(
            relationAnalyzer,
            showCreateTableAnalyzer,
            showStatementAnalyzer,
            deleteAnalyzer,
            updateAnalyzer,
            insertFromValuesAnalyzer,
            insertFromSubQueryAnalyzer,
            explainStatementAnalyzer
        );
    }

    public AnalyzedStatement analyze(Statement statement, SessionContext sessionContext, ParamTypeHints paramTypeHints) {
        CoordinatorTxnCtx coordinatorTxnCtx = new CoordinatorTxnCtx(sessionContext);
        return dispatcher.process(statement, new Analysis(coordinatorTxnCtx, ParameterContext.EMPTY, paramTypeHints));
    }

    private static class UnboundDispatcher extends AstVisitor<AnalyzedStatement, Analysis> {

        private final RelationAnalyzer relationAnalyzer;
        private final ShowCreateTableAnalyzer showCreateTableAnalyzer;
        private final ShowStatementAnalyzer showStatementAnalyzer;
        private final DeleteAnalyzer deleteAnalyzer;
        private final UpdateAnalyzer updateAnalyzer;
        private final InsertFromValuesAnalyzer insertFromValuesAnalyzer;
        private final InsertFromSubQueryAnalyzer insertFromSubQueryAnalyzer;
        private final ExplainStatementAnalyzer explainStatementAnalyzer;

        UnboundDispatcher(RelationAnalyzer relationAnalyzer,
                          ShowCreateTableAnalyzer showCreateTableAnalyzer,
                          ShowStatementAnalyzer showStatementAnalyzer,
                          DeleteAnalyzer deleteAnalyzer,
                          UpdateAnalyzer updateAnalyzer,
                          InsertFromValuesAnalyzer insertFromValuesAnalyzer,
                          InsertFromSubQueryAnalyzer insertFromSubQueryAnalyzer,
                          ExplainStatementAnalyzer explainStatementAnalyzer) {
            this.relationAnalyzer = relationAnalyzer;
            this.showCreateTableAnalyzer = showCreateTableAnalyzer;
            this.showStatementAnalyzer = showStatementAnalyzer;
            this.deleteAnalyzer = deleteAnalyzer;
            this.updateAnalyzer = updateAnalyzer;
            this.insertFromValuesAnalyzer = insertFromValuesAnalyzer;
            this.insertFromSubQueryAnalyzer = insertFromSubQueryAnalyzer;
            this.explainStatementAnalyzer = explainStatementAnalyzer;
        }

        @Override
        public AnalyzedStatement visitInsertFromSubquery(InsertFromSubquery insert, Analysis analysis) {
            return insertFromSubQueryAnalyzer.analyze(insert, analysis.paramTypeHints(), analysis.transactionContext());
        }

        @Override
        public AnalyzedStatement visitInsertFromValues(InsertFromValues insert, Analysis analysis) {
            return insertFromValuesAnalyzer.analyze(insert, analysis.paramTypeHints(), analysis.transactionContext());
        }

        @Override
        protected AnalyzedStatement visitQuery(Query node, Analysis context) {
            return relationAnalyzer.analyzeUnbound(
                node, context.transactionContext(), context.paramTypeHints());
        }

        @Override
        public AnalyzedStatement visitShowTransaction(ShowTransaction showTransaction, Analysis context) {
            Query query = showStatementAnalyzer.rewriteShowTransaction();
            return relationAnalyzer.analyzeUnbound(
                query, context.transactionContext(), ParamTypeHints.EMPTY);
        }

        @Override
        public AnalyzedStatement visitBegin(BeginStatement node, Analysis context) {
            return new AnalyzedBegin();
        }

        @Override
        protected AnalyzedStatement visitShowTables(ShowTables node, Analysis context) {
            ParameterContext parameterContext = context.parameterContext();
            Query query = showStatementAnalyzer.rewriteShowTables(node);
            return relationAnalyzer.analyzeUnbound(
                query, context.transactionContext(), parameterContext.typeHints());
        }

        @Override
        protected AnalyzedStatement visitShowSchemas(ShowSchemas node, Analysis context) {
            Query query = showStatementAnalyzer.rewriteShowSchemas(node);
            return relationAnalyzer.analyzeUnbound(query,
                context.transactionContext(),
                context.parameterContext().typeHints());
        }

        @Override
        protected AnalyzedStatement visitShowColumns(ShowColumns node, Analysis context) {
            CoordinatorTxnCtx coordinatorTxnCtx = context.transactionContext();
            Query query = showStatementAnalyzer.rewriteShowColumns(node,
                coordinatorTxnCtx.sessionContext().searchPath().currentSchema());
            return relationAnalyzer.analyzeUnbound(
                query, coordinatorTxnCtx, context.parameterContext().typeHints());
        }

        @Override
        public AnalyzedStatement visitShowCreateTable(ShowCreateTable node, Analysis context) {
            return showCreateTableAnalyzer.analyze(node.table(), context.sessionContext());
        }

        @Override
        public AnalyzedStatement visitShowSessionParameter(ShowSessionParameter node, Analysis context) {
            ShowStatementAnalyzer.validateSessionSetting(node.parameter());
            Query query = ShowStatementAnalyzer.rewriteShowSessionParameter(node);
            return relationAnalyzer.analyzeUnbound(query,
                context.transactionContext(),
                context.parameterContext().typeHints());
        }

        @Override
        public AnalyzedStatement visitDelete(Delete node, Analysis analysis) {
            return deleteAnalyzer.analyze(node, analysis.paramTypeHints(), analysis.transactionContext());
        }

        @Override
        public AnalyzedStatement visitUpdate(Update update, Analysis analysis) {
            return updateAnalyzer.analyze(update, analysis.paramTypeHints(), analysis.transactionContext());
        }

        @Override
        protected AnalyzedStatement visitExplain(Explain node, Analysis context) {
            return explainStatementAnalyzer.analyze(node, context);
        }
    }
}
