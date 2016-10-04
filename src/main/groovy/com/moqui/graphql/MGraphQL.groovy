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
import groovy.transform.CompileStatic
import org.moqui.context.ExecutionContextFactory
import org.moqui.context.ResourceReference
import org.moqui.jcache.MCache
import org.moqui.util.MNode
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import graphql.schema.GraphQLObjectType
import graphql.schema.GraphQLSchema

import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock

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
    final MCache<String, GraphQLSchema> schemaCache

    protected final Lock locationLoadLock = new ReentrantLock()

    final String graphQLSchemaDefCacheName = "service.graphql.schema.definition"
    final String graphQLSchemaCacheName = "service.graphql.schema"

    protected GraphQLSchema schema = null

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
        schemaCache = ecf.getCache().getLocalCache(graphQLSchemaCacheName)
        schema = GraphQLSchema.newSchema()
                .query(queryType)
                .mutation(mutationType)
                .build()
    }

    GraphQLSchema getSchema() {
        return schema
    }


    synchronized void loadSchemaNode(String schemaName) {
        if (schemaName != null) {
            GraphQLSchemaDefinition schemaDef = schemaDefCache.get(schemaName)
            if (schemaDef != null) {
                GraphQLSchema schema = schemaCache.get(schemaName)
                if (schema != null) return
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

                    // TODO Parse .graphql.xml, add type to schema
                    MNode schemaNode = MNode.parse(rr)
                    if (schemaName == null || schemaName == schemaNode.attribute("name")) {
                        GraphQLSchemaDefinition schemaDef = new GraphQLSchemaDefinition(this.ecf.getService(), schemaNode)
                        schemaDefCache.put(schemaDef.schemaName, schemaDef)
                    }
                }

            } else {
                logger.warn("Can't load MGraphQL APIs from component at [${serviceDirRr.location}] because it doesn't support exists/directory/etc")
            }
        }
    }

    synchronized GraphQLSchema initializeSchema(GraphQLSchemaDefinition schemaDef) {
        schemaDef.populateSortedTypes()

    }




    List<ResourceReference> getAllGraphQLFileLocations() {
        List<ResourceReference> graphQLRrList = new LinkedList()
        graphQLRrList.addAll(getComponentGraphQLFileLocations(null))
        return graphQLRrList
    }

    List<ResourceReference> getComponentGraphQLFileLocations(List<String> componentNameList) {
        List<ResourceReference> graphQLRrList = new LinkedList()

        List<String> componentBaseLocations
        if(componentNameList) {
            componentBaseLocations = []
            for (String cn in componentNameList)
                componentBaseLocations.add(ecf.getComponentBaseLocations().get(cn))
        } else {
            componentBaseLocations = new ArrayList(ecf.getComponentBaseLocations().values())
        }

        for (String location in componentBaseLocations) {
            ResourceReference serviceDirRr = ecf.getResource().getLocationReference(location + "/service")
            if (serviceDirRr.supportsAll()) {
                // if directory does not exist, skip it. component does not have a service directory
                if (!serviceDirRr.exists || !serviceDirRr.isDirectory()) continue
                for (ResourceReference rr in serviceDirRr.directoryEntries) {
                    if (!rr.fileName.endsWith(".graphql.xml")) continue
                    graphQLRrList.add(rr)
                }
            } else {
                logger.warn("Cannot load service directory in component location [${location}] because protocol [${serviceDirRr.uri.scheme}] is not supported.")
            }
        }

        return graphQLRrList
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
