package com.moqui.impl.service

import org.moqui.impl.context.ExecutionContextFactoryImpl
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import graphql.schema.GraphQLObjectType
import graphql.schema.GraphQLSchema

import static graphql.Scalars.GraphQLString
import static graphql.schema.GraphQLFieldDefinition.newFieldDefinition
import static graphql.schema.GraphQLObjectType.newObject

class GraphQLApi {
    protected final static Logger logger = LoggerFactory.getLogger(GraphQLApi.class)

    @SuppressWarnings("GrFinalVariableAccess")
    protected final ExecutionContextFactoryImpl ecfi

    GraphQLApi (ExecutionContextFactoryImpl ecfi) {
        this.ecfi = ecfi

        GraphQLObjectType queryType = newObject()
                .name("helloWorldQuery")
                .field(newFieldDefinition()
                        .type(GraphQLString)
                        .name("hello")
                        .staticValue("world")
                        .build())
                .build()

        GraphQLSchema schema = GraphQLSchema.newSchema()
                .query(queryType)
                .build()
    }
}