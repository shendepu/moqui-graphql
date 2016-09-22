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

import groovy.transform.CompileStatic
import org.moqui.context.ExecutionContextFactory
import org.moqui.context.ResourceReference
import org.moqui.jcache.MCache
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
    final MCache<String, ResourceNode> rootResourceCache

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
        schema = GraphQLSchema.newSchema()
                .query(queryType)
                .mutation(mutationType)
                .build()
    }

    GraphQLSchema getSchema() {
        return schema
    }


    synchronized void loadRootResourceNode(String name) {
        if (name != null) {
            ResourceNode resourceNode = rootResourceCache.get(name)
            if (resourceNode != null) return
        }

        long startTime = System.currentTimeMillis()
        // find *.rest.xml files in component/service directories, put in rootResourceMap
        for (String location in this.ecf.getComponentBaseLocations().values()) {
            ResourceReference serviceDirRr = this.ecf.getResource().getLocationReference(location + "/service")
            if (serviceDirRr.supportsAll()) {
                // if for some weird reason this isn't a directory, skip it
                if (!serviceDirRr.isDirectory()) continue
                for (ResourceReference rr in serviceDirRr.directoryEntries) {
                    if (!rr.fileName.endsWith(".graphql.xml")) continue
                }

                // TODO Parse .graphql.xml, add type to schema
            } else {
                logger.warn("Can't load MGraphQL APIs from component at [${serviceDirRr.location}] because it doesn't support exists/directory/etc")
            }
        }

    }

    static class ResourceNode {

    }

}
