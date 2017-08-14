/*
 * This software is in the public domain under CC0 1.0 Universal plus a
 * Grant of Patent License.
 *
 * To the extent possible under law, the author(s) have dedicated all
 * copyright and related and neighboring rights to this software to the
 * public domain worldwide. This software is distributed without any
 * warranty.
 *
 * You should have received a copy of the CC0 Public Domain Dedication
 * along with this software (see the LICENSE.md file). If not, see
 * <http://creativecommons.org/publicdomain/zero/1.0/>.
 */
package com.moqui.graphql

import graphql.GraphQLException
import graphql.language.IntValue
import graphql.language.StringValue
import graphql.schema.Coercing
import graphql.schema.GraphQLScalarType
import groovy.transform.CompileStatic

import java.sql.Timestamp

@CompileStatic
class Scalars {
    private static final BigInteger LONG_MAX = BigInteger.valueOf(Long.MAX_VALUE)
    private static final BigInteger LONG_MIN = BigInteger.valueOf(Long.MIN_VALUE)

    public static GraphQLScalarType GraphQLTimestamp = new GraphQLScalarType("Timestamp", "Timestamp Type", new Coercing() {
        @Override
        Object serialize(Object input) {
            if (input instanceof String) {
                if (input == "" || input == null) return null
                return Timestamp.valueOf(input).getTime()
            } else if (input instanceof Long) {
                return new Timestamp(input).getTime()
            } else if (input instanceof Timestamp) {
                return input.getTime()
            }
            return null
        }

        @Override
        Object parseValue(Object input) {
            if (input instanceof String) {
                return Timestamp.valueOf(input)
            } else if (input instanceof Long) {
                return new Timestamp(input)
            } else if (input instanceof Timestamp) {
                return input
            }
            return null
        }

        @Override
        Object parseLiteral(Object input) {
            if (input instanceof StringValue) {
                return Timestamp.valueOf(((StringValue) input).getValue())
            } else if (input instanceof IntValue) {
                BigInteger value = ((IntValue) input).getValue()
                // Check if out of bounds.
                if (value.compareTo(LONG_MIN) < 0 || value.compareTo(LONG_MAX) > 0) {
                    throw new GraphQLException("Int literal is too big or too small for a long, would cause overflow");
                }
                return new Timestamp(value.longValue())
            }
            return null
        }
    })
}
