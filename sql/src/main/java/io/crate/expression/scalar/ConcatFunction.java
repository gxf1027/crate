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

package io.crate.expression.scalar;

import com.google.common.collect.Lists;
import io.crate.data.Input;
import io.crate.exceptions.ConversionException;
import io.crate.expression.symbol.FuncArg;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.FunctionName;
import io.crate.metadata.FunctionResolver;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.functions.params.FuncParams;
import io.crate.metadata.functions.params.Param;
import io.crate.metadata.functions.params.TypeParam;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.StringType;

import javax.annotation.Nullable;
import java.util.List;

public abstract class ConcatFunction extends Scalar<String, String> {

    public static final String NAME = "concat";
    private FunctionInfo functionInfo;

    public static void register(ScalarFunctionModule module) {
        //module.register(NAME, new Resolver());

        // V2
        FunctionName name = new FunctionName(null, NAME);

        var str = new TypeParam("any_str", t -> t.id() == StringType.ID);
        module.register(
            name,
            new Signature(str, str),
            new StringConcatFunction(
                new FunctionInfo(
                    new FunctionIdent(NAME, List.of(StringType.INSTANCE, StringType.INSTANCE)),
                    DataTypes.STRING)));

        var str_variadic = new TypeParam("any_str_var", t -> t.id() == StringType.ID, TypeParam.Mode.VARIADIC);
        module.register(
            name,
            new Signature(str, str_variadic),
            new GenericConcatFunction(
                new FunctionInfo(
                    new FunctionIdent(NAME, List.of(StringType.INSTANCE, StringType.INSTANCE)),
                    DataTypes.STRING)));

        for (DataType<?> dataType : DataTypes.PRIMITIVE_TYPES) {
            var arrType = new ArrayType<>(dataType);
            var arr = new TypeParam(
                "any_array",
                t -> t.id() == ArrayType.ID && ((ArrayType) t).innerType().id() == dataType.id());
            module.register(
                name,
                new Signature(
                    List.of(arr, arr),
                    args -> ArrayCatFunction.validateInnerTypes(Lists.transform(args, FuncArg::valueType))),
                new ArrayCatFunction(
                    ArrayCatFunction.createInfo(List.of(arrType, arrType))
                ));
        }
    }

    ConcatFunction(FunctionInfo functionInfo) {
        this.functionInfo = functionInfo;
    }

    @Override
    public FunctionInfo info() {
        return functionInfo;
    }

    @Override
    public Symbol normalizeSymbol(Function function, TransactionContext txnCtx) {
        if (anyNonLiterals(function.arguments())) {
            return function;
        }
        Input[] inputs = new Input[function.arguments().size()];
        for (int i = 0; i < function.arguments().size(); i++) {
            inputs[i] = ((Input) function.arguments().get(i));
        }
        //noinspection unchecked
        return Literal.ofUnchecked(functionInfo.returnType(), evaluate(txnCtx, inputs));
    }

    private static class StringConcatFunction extends ConcatFunction {

        StringConcatFunction(FunctionInfo functionInfo) {
            super(functionInfo);
        }

        @Override
        public String evaluate(TransactionContext txnCtx, Input[] args) {
            String firstArg = (String) args[0].value();
            String secondArg = (String) args[1].value();
            if (firstArg == null) {
                if (secondArg == null) {
                    return "";
                }
                return secondArg;
            }
            if (secondArg == null) {
                return firstArg;
            }
            return firstArg + secondArg;
        }
    }

    private static class GenericConcatFunction extends ConcatFunction {

        GenericConcatFunction(FunctionInfo functionInfo) {
            super(functionInfo);
        }

        @Override
        public String evaluate(TransactionContext txnCtx, Input<String>[] args) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < args.length; i++) {
                String value = args[i].value();
                if (value != null) {
                    sb.append(value);
                }
            }
            return sb.toString();
        }
    }

    private static class Resolver implements FunctionResolver {

        private static final FuncParams STRINGS = FuncParams
            .builder(Param.STRING, Param.STRING)
            .withVarArgs(Param.STRING)
            .build();
        private static final FuncParams ARRAYS = FuncParams
            .builder(
                Param.of(new ArrayType<>(DataTypes.UNDEFINED)).withInnerType(Param.ANY),
                Param.of(new ArrayType<>(DataTypes.UNDEFINED)).withInnerType(Param.ANY)
            )
            .build();

        @Nullable
        @Override
        public List<DataType> getSignature(List<? extends FuncArg> funcArgs) {
            try {
                return STRINGS.match(funcArgs);
            } catch (ConversionException e1) {
                try {
                    return ARRAYS.match(funcArgs);
                } catch (ConversionException e2) {
                    throw e1; // prefer error message with casts to string instead of casts to arrays
                }
            }
        }

        @Override
        public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            if (dataTypes.size() == 2 && dataTypes.get(0).equals(DataTypes.STRING) &&
                       dataTypes.get(1).equals(DataTypes.STRING)) {
                return new StringConcatFunction(new FunctionInfo(new FunctionIdent(NAME, dataTypes), DataTypes.STRING));
            } else if (dataTypes.size() == 2 && dataTypes.get(0) instanceof ArrayType &&
                       dataTypes.get(1) instanceof ArrayType) {

                ArrayCatFunction.validateInnerTypes(dataTypes);
                return new ArrayCatFunction(ArrayCatFunction.createInfo(dataTypes));
            } else {
                return new GenericConcatFunction(new FunctionInfo(new FunctionIdent(NAME, dataTypes), DataTypes.STRING));
            }
        }
    }
}
