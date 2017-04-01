package com.moqui.impl.service.fetcher

import com.moqui.impl.util.GraphQLSchemaUtil
import graphql.schema.DataFetchingEnvironment
import groovy.transform.CompileStatic
import org.moqui.context.ExecutionContext
import org.moqui.context.ExecutionContextFactory
import org.moqui.entity.EntityValue
import org.moqui.impl.context.ExecutionContextFactoryImpl
import org.moqui.impl.context.UserFacadeImpl
import org.moqui.impl.service.ServiceDefinition
import org.moqui.util.MNode
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static com.moqui.impl.service.GraphQLSchemaDefinition.FieldDefinition

@CompileStatic
class ServiceDataFetcher extends BaseDataFetcher {
    protected final static Logger logger = LoggerFactory.getLogger(ServiceDataFetcher.class)

    String serviceName
    String requireAuthentication
    boolean isEntityAutoService
    ServiceDefinition sd
    Map<String, String> relKeyMap = new HashMap<>()

    ServiceDataFetcher(MNode node, FieldDefinition fieldDef, ExecutionContextFactory ecf) {
        super(fieldDef, ecf)
        this.requireAuthentication = node.attribute("require-authentication") ?: fieldDef.requireAuthentication ?: "true"

        this.serviceName = node.attribute("service")

        for (MNode keyMapNode in node.children("key-map"))
            relKeyMap.put(keyMapNode.attribute("field-name"), keyMapNode.attribute("related") ?: keyMapNode.attribute("field-name"))

        this.isEntityAutoService = ((ExecutionContextFactoryImpl) ecf).serviceFacade.isEntityAutoPattern(serviceName)
        if (this.isEntityAutoService) {
            if (!fieldDef.isMutation) throw new IllegalArgumentException("Query should not use entity auto service ${serviceName}")
        } else {
            sd = ((ExecutionContextFactoryImpl) ecf).serviceFacade.getServiceDefinition(serviceName)
            if (sd == null) throw new IllegalArgumentException("Service ${serviceName} not found")
        }
    }

    @Override
    Object fetch(DataFetchingEnvironment environment) {
        logger.info("---- running data fetcher service [${serviceName}] ...")
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
            Map<String, Object> inputFieldsMap = new HashMap<>()
            if (fieldDef.isMutation) {
                GraphQLSchemaUtil.transformArguments(environment.arguments, inputFieldsMap)
            }
            else {
                GraphQLSchemaUtil.transformQueryServiceArguments(sd, environment.arguments, inputFieldsMap)
                logger.info("environment.source: ${environment.source}")
                Map source = environment.source as Map<String, Object>
                GraphQLSchemaUtil.transformQueryServiceRelArguments(source, relKeyMap, inputFieldsMap)
            }
            logger.info("inputFieldsMap - ${inputFieldsMap}")

            Map result
            if (fieldDef.isMutation) {
                result = ec.getService().sync().name(serviceName).parameters(inputFieldsMap).call()
                if (this.isEntityAutoService) {
                    String verb = ServiceDefinition.getVerbFromName(serviceName)
                    if (verb == 'delete') { // delete return result object { error, message }
                        result = [ error: false, message: '' ]
                    } else {
                        String entityName = ServiceDefinition.getNounFromName(serviceName)
                        EntityValue ev = ec.getEntity().find(entityName).condition(result).one()
                        if (ev) {
                            result = ec.getService().sync().name("graphql.GraphQLServices.put#ContextWithId").parameter("ev", ev).call()
                        }
                    }
                }
            } else {
                result = ec.getService().sync().name(serviceName)
                        .parameter("environment", environment)
                        .parameters(inputFieldsMap).call()
            }
            if (result && result.get("_graphql_result_null")) return null
            return result
        } finally {
            if (loggedInAnonymous) ((UserFacadeImpl) ec.getUser()).logoutAnonymousOnly()
        }
    }
}
