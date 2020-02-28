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

package io.crate.metadata.functions;

import com.google.common.collect.Lists;
import io.crate.expression.symbol.FuncArg;
import io.crate.metadata.functions.params.TypeParam;

import java.util.List;
import java.util.function.Consumer;

public class Signature {

    private final List<TypeParam> params;
    private final Consumer<List<? extends FuncArg>> validation;

    public Signature(TypeParam... params) {
        this(Lists.newArrayList(params));
    }

    public Signature(List<TypeParam> params) {
        this(params, a -> { });
    }

    public Signature(List<TypeParam> params, Consumer<List<? extends FuncArg>> validation) {
        this.params = params;
        this.validation = validation;
    }

    public boolean match(List<? extends FuncArg> args) {
        if (params.size() == 0) {
            return args.size() == 0;
        }
        TypeParam lastParam = null;
        for (int i = 0; i < args.size(); i++) {
            FuncArg arg = args.get(i);
            if (i < params.size()) {
                var param = lastParam = params.get(i);
                if (!param.match(arg.valueType())) {
                    return false;
                }
            } else {
                if (lastParam.mode() != TypeParam.Mode.VARIADIC) {
                    return false;
                }
                if (!lastParam.match(arg.valueType())) {
                    return false;
                }
            }
        }
        validation.accept(args);
        return true;
    }
}
