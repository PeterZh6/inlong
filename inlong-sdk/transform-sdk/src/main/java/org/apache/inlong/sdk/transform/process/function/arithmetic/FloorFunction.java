/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sdk.transform.process.function.arithmetic;

import org.apache.inlong.sdk.transform.decode.SourceData;
import org.apache.inlong.sdk.transform.process.Context;
import org.apache.inlong.sdk.transform.process.function.TransformFunction;
import org.apache.inlong.sdk.transform.process.operator.OperatorTools;
import org.apache.inlong.sdk.transform.process.parser.ValueParser;

import net.sf.jsqlparser.expression.Function;

import java.math.BigDecimal;

/**
 * FloorFunction  ->  floor(numeric)
 * Description:
 * - Return NULL if 'numeric' is NULL;
 * - Return the largest number that is less than or equal to 'numeric'.
 */
@TransformFunction(names = {"floor"}, parameter = "(Numeric numeric)", descriptions = {
        "- Return \"\" if 'numeric' is NULL;",
        "- Return the largest number that is less than or equal to 'numeric'."
}, examples = {
        "floor(1.23) = 1.0",
        "floor(-5.67) = -6.0"
})
public class FloorFunction implements ValueParser {

    private ValueParser numberParser;

    public FloorFunction(Function expr) {
        numberParser = OperatorTools.buildParser(expr.getParameters().getExpressions().get(0));
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object numberObj = numberParser.parse(sourceData, rowIndex, context);
        if (numberObj == null) {
            return null;
        }
        BigDecimal numberValue = OperatorTools.parseBigDecimal(numberObj);
        return Math.floor(numberValue.doubleValue());
    }
}
