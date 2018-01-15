package com.moqui.impl.service.fetcher

import com.moqui.impl.util.GraphQLSchemaUtil
import graphql.schema.DataFetchingEnvironment
import groovy.transform.CompileStatic
import org.moqui.context.ExecutionContext
import org.moqui.context.ExecutionContextFactory
import org.moqui.entity.EntityCondition
import org.moqui.entity.EntityFind
import org.moqui.entity.EntityList
import org.moqui.entity.EntityValue
import org.moqui.impl.context.UserFacadeImpl
import org.moqui.util.MNode

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static com.moqui.impl.service.GraphQLSchemaDefinition.FieldDefinition

@CompileStatic
@Deprecated
class EntityDataFetcher extends BaseEntityDataFetcher {
    protected final static Logger logger = LoggerFactory.getLogger(EntityDataFetcher.class)

    EntityDataFetcher(MNode node, FieldDefinition fieldDef, ExecutionContextFactory ecf) {
        super(node, fieldDef, ecf)
    }

    EntityDataFetcher(ExecutionContextFactory ecf, FieldDefinition fieldDef, String entityName, Map<String, String> relKeyMap) {
        this(ecf, fieldDef, entityName, null, relKeyMap)
    }

    EntityDataFetcher(ExecutionContextFactory ecf, FieldDefinition fieldDef, String entityName, String interfaceEntityName, Map<String, String> relKeyMap) {
        super(ecf, fieldDef, entityName, interfaceEntityName, relKeyMap)
    }

    @Override
    Object fetch(DataFetchingEnvironment environment) {
        logger.info("---- running data fetcher entity for entity [${entityName}] with operation [${operation}] ...")
//        logger.info("source     - ${environment.source}")
//        logger.info("arguments  - ${environment.arguments}")
//        logger.info("context    - ${environment.context}")
//        logger.info("fields     - ${environment.fields}")
//        logger.info("fieldType  - ${environment.fieldType}")
//        logger.info("parentType - ${environment.parentType}")
//        logger.info("relKeyMap  - ${relKeyMap}")
//        logger.info("interfaceEntityName    - ${interfaceEntityName}")

        ExecutionContext ec = environment.context as ExecutionContext

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
            GraphQLSchemaUtil.transformArguments(environment.arguments, inputFieldsMap)
            logger.info("pageIndex   - ${inputFieldsMap.get('pageIndex')}")
            logger.info("pageSize    - ${inputFieldsMap.get('pageSize')}")
            if (operation == "one") {
                EntityFind ef = ec.entity.find(entityName).searchFormMap(inputFieldsMap, null, null, null, false)
                for (Map.Entry<String, String> entry in relKeyMap.entrySet()) {
                    ef = ef.condition(entry.getValue(), ((Map) environment.source).get(entry.getKey()))
                }
                EntityValue one = ef.one()
                if (one == null) return null
                if (interfaceEntityName == null || interfaceEntityName.isEmpty() || entityName.equals(interfaceEntityName)) {
                    return one.getMap()
                } else {

                    logger.info("entity find interface EntityName - ${interfaceEntityName}")
                    logger.info("entity find one.getPrimaryKeys - ${one.getPrimaryKeys()}")
                    ef = ec.entity.find(interfaceEntityName)
                            .condition(ec.getEntity().getConditionFactory().makeCondition(one.getPrimaryKeys()))
                    EntityValue interfaceOne = ef.one()
                    Map jointOneMap = new HashMap()
                    if (interfaceOne != null) jointOneMap.putAll(interfaceOne.getMap())
                    jointOneMap.putAll(one.getMap())

                    return jointOneMap
                }
            } else if (operation == "list") {
                EntityFind ef = ec.entity.find(entityName).searchFormMap(inputFieldsMap, null, null, null, true)
                for (Map.Entry<String, String> entry in relKeyMap.entrySet()) {
                    ef = ef.condition(entry.getValue(), ((Map) environment.source).get(entry.getKey()))
                }

                if (!ef.getLimit()) ef.limit(100)

                int count = ef.count() as int
                int pageIndex = ef.getPageIndex()
                int pageSize = ef.getPageSize()
                int pageMaxIndex = ((count - 1) as BigDecimal).divide(pageSize as BigDecimal, 0, BigDecimal.ROUND_DOWN).intValue()
                int pageRangeLow = pageIndex * pageSize + 1
                int pageRangeHigh = (pageIndex * pageSize) + pageSize
                if (pageRangeHigh > count) pageRangeHigh = count
                boolean hasPreviousPage = pageIndex > 0
                boolean hasNextPage = pageMaxIndex > pageIndex

                Map<String, Object> resultMap = new HashMap<>()
                Map<String, Object> pageInfo = ['pageIndex'      : pageIndex, 'pageSize': pageSize, 'totalCount': count,
                                                'pageMaxIndex'   : pageMaxIndex, 'pageRangeLow': pageRangeLow, 'pageRangeHigh': pageRangeHigh,
                                                'hasPreviousPage': hasPreviousPage, 'hasNextPage': hasNextPage] as Map<String, Object>

                EntityList el = ef.list()
                List<Map<String, Object>> edgesDataList = new ArrayList(el.size())
                Map<String, Object> edgesData
                String cursor

                if (el == null || el.size() == 0) {
                    // Do nothing
                } else {
                    if (interfaceEntityName == null || interfaceEntityName.isEmpty() || entityName.equals(interfaceEntityName)) {
                        pageInfo.put("startCursor", GraphQLSchemaUtil.encodeRelayCursor(el.get(0), pkFieldNames))
                        pageInfo.put("endCursor", GraphQLSchemaUtil.encodeRelayCursor(el.get(el.size() - 1), pkFieldNames))
                        for (EntityValue ev in el) {
                            edgesData = new HashMap<>(2)
                            cursor = GraphQLSchemaUtil.encodeRelayCursor(ev, pkFieldNames)
                            edgesData.put("cursor", cursor)
                            edgesData.put("node", ev.getMap())
                            edgesDataList.add(edgesData)
                        }
                    } else {
                        List<Object> pkValues = new ArrayList<>()
                        for (EntityValue ev in el) pkValues.add(ev.get(interfaceEntityPkField))

                        EntityFind efInterface = ec.entity.find(interfaceEntityName).condition(interfaceEntityPkField, EntityCondition.ComparisonOperator.IN, pkValues)

                        Map<String, Object> jointOneMap, matchedOne

                        pageInfo.put("startCursor", GraphQLSchemaUtil.encodeRelayCursor(el.get(0), pkFieldNames))
                        pageInfo.put("endCursor", GraphQLSchemaUtil.encodeRelayCursor(el.get(el.size() - 1), pkFieldNames))
                        for (EntityValue ev in el) {
                            edgesData = new HashMap<>(2)
                            cursor = GraphQLSchemaUtil.encodeRelayCursor(ev, pkFieldNames)
                            edgesData.put("cursor", cursor)
                            jointOneMap = ev.getMap()
                            matchedOne = efInterface.list().find({ ((EntityValue) it).get(interfaceEntityPkField).equals(ev.get(interfaceEntityPkField)) })
                            jointOneMap.putAll(matchedOne)
                            edgesData.put("node", jointOneMap)
                            edgesDataList.add(edgesData)
                        }
                    }
                }
                resultMap.put("edges", edgesDataList)
                resultMap.put("pageInfo", pageInfo)
                return resultMap
            }
        }
        finally {
            if (loggedInAnonymous) ((UserFacadeImpl) ec.getUser()).logoutAnonymousOnly()
        }
        return null
    }

}