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

import com.moqui.impl.service.GraphQLSchemaDefinition
import graphql.ExecutionResult
import graphql.GraphQL
import groovy.transform.CompileStatic
import org.moqui.context.ExecutionContextFactory
import org.moqui.context.ResourceReference
import org.moqui.jcache.MCache
import org.moqui.util.MNode
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import graphql.schema.GraphQLObjectType
import graphql.schema.GraphQLSchema

import static graphql.schema.GraphQLFieldDefinition.newFieldDefinition
import static graphql.Scalars.GraphQLString

@CompileStatic
class MGraphQL {
    protected final static Logger logger = LoggerFactory.getLogger(MGraphQL.class)

    @SuppressWarnings("GrFinalVariableAccess")
    protected final ExecutionContextFactory ecf
    @SuppressWarnings("GrFinalVariableAccess")
    final MCache<String, GraphQLSchemaDefinition> schemaDefCache
    @SuppressWarnings("GrFinalVariableAccess")
    final MCache<String, GraphQL> graphQLCache

    final String graphQLSchemaDefCacheName = "service.graphql.schema.definition"
    final String graphQLCacheName = "service.graphql.graphql"

    static GraphQLObjectType queryType = GraphQLObjectType.newObject()
            .name("RootQueryType")
            .field(newFieldDefinition()
                    .name("products")
                    .type(GraphQLString)
                    .staticValue("product list")
                    .build())
            .build()
    static GraphQLObjectType mutationType = GraphQLObjectType.newObject().name("RootMutationType").build()


    MGraphQL(ExecutionContextFactory ecf) {
        this.ecf = ecf
        schemaDefCache = ecf.getCache().getLocalCache(graphQLSchemaDefCacheName)
        graphQLCache = ecf.getCache().getLocalCache(graphQLCacheName)

        loadSchemaNode(null)
    }

    GraphQLResult execute(String schemaName, String requestString) {
        return execute(schemaName, requestString, null, (Object) null, Collections.<String, Object> emptyMap())
    }

    GraphQLResult execute(String schemaName, String requestString, Map<String, Object> arguments) {
        return execute(schemaName, requestString, null, (Object) null, arguments)
    }

    GraphQLResult execute(String schemaName, String requestString, String operationName, Object context, Map<String, Object> arguments) {
        GraphQL graphQL = graphQLCache.get(schemaName)

        if (graphQL == null) {
            loadSchemaNode(schemaName)
            graphQL = graphQLCache.get(schemaName)
        }

        ExecutionResult executionResult = graphQL.execute("${requestString}", operationName, context, arguments)
        return new GraphQLResult(executionResult)
    }

    synchronized void loadSchemaNode(String schemaName) {
        if (schemaName != null) {
            GraphQLSchemaDefinition schemaDef = schemaDefCache.get(schemaName)
            if (schemaDef != null) {
                GraphQL graphQL = graphQLCache.get(schemaName)
                if (graphQL != null) return
            }
        }

        long startTime = System.currentTimeMillis()
        // find *.graphql.xml files in component/service directories
        for (String location in this.ecf.getComponentBaseLocations().values()) {
            ResourceReference serviceDirRr = this.ecf.getResource().getLocationReference(location + "/service")
            if (serviceDirRr.supportsAll()) {
                // if for some weird reason this isn't a directory, skip it
                if (!serviceDirRr.exists || !serviceDirRr.isDirectory()) continue
                for (ResourceReference rr in serviceDirRr.directoryEntries) {
                    if (!rr.fileName.endsWith(".graphql.xml")) continue

                    logger.info("Loading ${rr.fileName}")
                    // Parse .graphql.xml, add schema to cache
                    MNode schemaNode = MNode.parse(rr)
                    if (schemaName == null || schemaName == schemaNode.attribute("name")) {
                        GraphQLSchemaDefinition schemaDef = new GraphQLSchemaDefinition(this.ecf.getService(), schemaNode)
                        schemaDefCache.put(schemaDef.schemaName, schemaDef)
                        graphQLCache.put(schemaDef.schemaName, new GraphQL(schemaDef.getSchema()))
                    }
                }
            } else {
                logger.warn("Can't load GraphQL APIs from component at [${serviceDirRr.location}] because it doesn't support exists/directory/etc")
            }
        }
        logger.info("Loaded GraphQL API files, ${graphQLCache.size()} schemas, in ${System.currentTimeMillis() - startTime}ms")
    }

    static class GraphQLResult {
        Map responseObj = new HashMap<String, Object>()

        GraphQLResult(ExecutionResult executionResult) {
            if (executionResult.getErrors().size() > 0) {
                responseObj.put("errors", executionResult.getErrors())
            }
            responseObj.put("data", executionResult.getData())
        }

    }

}
