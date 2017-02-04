package com.moqui.impl.service.fetcher

import com.moqui.impl.util.GraphQLSchemaUtil
import graphql.schema.DataFetchingEnvironment
import org.moqui.context.ExecutionContext
import org.moqui.context.ExecutionContextFactory
import org.moqui.impl.context.UserFacadeImpl
import org.moqui.util.MNode

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static com.moqui.impl.service.GraphQLSchemaDefinition.FieldDefinition

class ElasticSearchDataFetcher extends BaseDataFetcher {
    protected final static Logger logger = LoggerFactory.getLogger(ElasticSearchDataFetcher.class)

    String dataDocumentId
    String requireAuthentication
    Map<String, Object> dataDocDefMap

    ElasticSearchDataFetcher(MNode node, FieldDefinition fieldDef, ExecutionContextFactory ecf) {
        super(fieldDef, ecf)
        this.requireAuthentication = node.attribute("require-authentication") ?: fieldDef.requireAuthentication ?: "true"
        this.dataDocumentId = node.attribute("data-document-id")

        boolean alreadyDisabled = ecf.executionContext.artifactExecution.disableAuthz()
        try {
            this.dataDocDefMap = GraphQLSchemaUtil.getDataDocDefinition(ecf, this.dataDocumentId)
        } finally {
            if (!alreadyDisabled) ecf.executionContext.artifactExecution.enableAuthz()
        }
    }

    @Override

    Object fetch(DataFetchingEnvironment environment) {
        logger.info("---- running data fetcher elastic search on data document [${dataDocumentId}] ...")

        ExecutionContext ec = ecf.getExecutionContext()
        boolean loggedInAnonymous = false
        if ("anonymous-all".equals(requireAuthentication)) {
            ec.artifactExecution.setAnonymousAuthorizedAll()
            loggedInAnonymous = ec.getUser().loginAnonymousIfNoUser()
        } else if ("anonymous-view".equals(requireAuthentication)) {
            ec.artifactExecution.setAnonymousAuthorizedView()
            loggedInAnonymous = ec.getUser().loginAnonymousIfNoUser()
        }

        try {

            return null
        } finally {
            if (loggedInAnonymous) ((UserFacadeImpl) ec.getUser()).logoutAnonymousOnly()
        }
    }
}
