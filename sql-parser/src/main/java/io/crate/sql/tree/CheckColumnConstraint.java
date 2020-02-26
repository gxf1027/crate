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

package io.crate.sql.tree;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import io.crate.sql.ExpressionFormatter;

import javax.annotation.Nullable;
import java.util.function.Consumer;
import java.util.function.Function;


public class CheckColumnConstraint<T> extends ColumnConstraint<T> {

    private final String name;
    private final String columnName;
    private final Expression expression;
    private final String expressionStr;

    public CheckColumnConstraint(@Nullable String name,
                                 @Nullable String columnName,
                                 Expression expression) {
        this.name = name;
        this.columnName = columnName;
        this.expression = expression;
        this.expressionStr = ExpressionFormatter.formatStandaloneExpression(expression);
    }

    public String columnName() {
        return columnName;
    }

    public String name() {
        return name;
    }

    public Expression expression() {
        return expression;
    }

    public String expressionStr() {
        return expressionStr;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name, columnName, expression);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (null == o || false == o instanceof CheckColumnConstraint) {
            return false;
        }
        CheckColumnConstraint that = (CheckColumnConstraint) o;
        return Objects.equal(expression, that.expression) &&
               Objects.equal(columnName, that.columnName) &&
               Objects.equal(name, that.name);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCheckColumnConstraint(this, context);
    }

    @Override
    public <U> ColumnConstraint<U> map(Function<? super T, ? extends U> mapper) {
        return new CheckColumnConstraint<>(name, columnName, expression);
    }

    @Override
    public void visit(Consumer<? super T> consumer) {
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("name", name)
            .add("column", columnName)
            .add("expression", expressionStr)
            .toString();
    }
}
