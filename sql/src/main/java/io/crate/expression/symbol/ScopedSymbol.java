/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.expression.symbol;

import io.crate.metadata.ColumnIdent;
import io.crate.sql.tree.QualifiedName;
import io.crate.types.DataType;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * TODO: update docs
 */
public final class ScopedSymbol extends Symbol {

    private final QualifiedName relation;
    private final ColumnIdent column;
    private final DataType<?> dataType;

    public ScopedSymbol(QualifiedName relation, ColumnIdent column, DataType<?> dataType) {
        this.relation = relation;
        this.column = column;
        this.dataType = dataType;
    }

    public QualifiedName relation() {
        return relation;
    }

    public ColumnIdent column() {
        return column;
    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.RELATION_OUTPUT;
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitField(this, context);
    }

    @Override
    public DataType<?> valueType() {
        return dataType;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("ScopedSymbol is not streamable");
    }

    @Override
    public String toString() {
        return representation();
    }

    @Override
    public String representation() {
        return relation.toString() + '.' + column.sqlFqn();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ScopedSymbol that = (ScopedSymbol) o;

        if (!relation.equals(that.relation)) {
            return false;
        }
        if (!column.equals(that.column)) {
            return false;
        }
        return dataType.equals(that.dataType);
    }

    @Override
    public int hashCode() {
        int result = relation.hashCode();
        result = 31 * result + column.hashCode();
        result = 31 * result + dataType.hashCode();
        return result;
    }
}
