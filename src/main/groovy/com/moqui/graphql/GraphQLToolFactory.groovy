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
package org.moqui.graphql

import com.moqui.graphql.MGraphQL
import graphql.GraphQL
import groovy.transform.CompileStatic
import org.moqui.context.ExecutionContextFactory
import org.moqui.context.ToolFactory
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@CompileStatic
class GraphQLToolFactory implements ToolFactory<GraphQL> {
    protected final static Logger logger = LoggerFactory.getLogger(GraphQLToolFactory.class)
    final static String TOOL_NAME = "GraphQL"

    protected ExecutionContextFactory ecf = null
    protected GraphQL graphql = null

    /** Default empty constructor */
    GraphQLToolFactory() { }

    @Override
    String getName() { return TOOL_NAME }

    @Override
    void init(ExecutionContextFactory ecf) {
        this.ecf = ecf
        MGraphQL mg = new MGraphQL(ecf)
        graphql = new GraphQL(mg.schema)
        logger.info("GraphQLToolFactory Initialized")
    }

    @Override
    void preFacadeInit(ExecutionContextFactory ecf) {}

    @Override
    GraphQL getInstance(Object... parameters) {
        if (graphql == null) throw new IllegalStateException("GraphQLToolFactory not initialized")
        return graphql
    }

    @Override
    void destroy() {

    }

    ExecutionContextFactory getEcf() { return ecf }
}