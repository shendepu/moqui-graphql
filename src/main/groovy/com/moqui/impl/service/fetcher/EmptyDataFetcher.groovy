package com.moqui.impl.service.fetcher

import com.moqui.impl.util.GraphQLSchemaUtil
import graphql.schema.DataFetchingEnvironment
import groovy.transform.CompileStatic
import org.moqui.util.MNode

import com.moqui.impl.service.GraphQLSchemaDefinition.FieldDefinition

@CompileStatic
class EmptyDataFetcher extends BaseDataFetcher {
    EmptyDataFetcher(MNode node, FieldDefinition fieldDef) {
        super(fieldDef, null)
    }

    EmptyDataFetcher(FieldDefinition fieldDef) {
        super(fieldDef, null)
    }

    @Override
    Object fetch(DataFetchingEnvironment environment) {
        if (!GraphQLSchemaUtil.graphQLScalarTypes.containsKey(fieldDef.type)) {
            if ("true".equals(fieldDef.isList)) {
                return new ArrayList<Object>()
            }
            return new HashMap<String, Object>()
        }
        return null
    }
}