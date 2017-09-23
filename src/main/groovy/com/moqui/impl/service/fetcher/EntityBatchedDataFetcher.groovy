package com.moqui.impl.service.fetcher

import com.moqui.impl.util.GraphQLSchemaUtil
import graphql.execution.batched.BatchedDataFetcher
import graphql.schema.DataFetchingEnvironment
import groovy.transform.CompileStatic
import org.moqui.context.ExecutionContext
import org.moqui.context.ExecutionContextFactory
import org.moqui.entity.EntityCondition
import org.moqui.entity.EntityCondition.ComparisonOperator
import org.moqui.entity.EntityCondition.JoinOperator
import org.moqui.entity.EntityFind
import org.moqui.entity.EntityList
import org.moqui.entity.EntityValue
import org.moqui.impl.context.UserFacadeImpl
import org.moqui.util.MNode

import static com.moqui.impl.service.GraphQLSchemaDefinition.FieldDefinition

@CompileStatic
class EntityBatchedDataFetcher extends BaseEntityDataFetcher implements BatchedDataFetcher {
    EntityBatchedDataFetcher(MNode node, FieldDefinition fieldDef, ExecutionContextFactory ecf) {
        super(node, fieldDef, ecf)

    }

    EntityBatchedDataFetcher(ExecutionContextFactory ecf, FieldDefinition fieldDef, String entityName, Map<String, String> relKeyMap) {
        this(ecf, fieldDef, entityName, null, relKeyMap)
    }

    EntityBatchedDataFetcher(ExecutionContextFactory ecf, FieldDefinition fieldDef, String entityName, String interfaceEntityName, Map<String, String> relKeyMap) {
        super(ecf, fieldDef, entityName, interfaceEntityName, relKeyMap)
    }

    private EntityFind getInterfaceEntityFind(ExecutionContext ec, EntityFind ef) {
        if (!requireInterfaceEntity()) return null
        List<Object> pkValues = new ArrayList<>()
        for (EntityValue ev in ef.list()) pkValues.add(ev.get(interfaceEntityPkField))
        return ec.entity.find(interfaceEntityName)
                .condition(interfaceEntityPkField, ComparisonOperator.IN, pkValues)
                .useCache(useCache)
    }

    private EntityValue getInterfaceEntityValue(ExecutionContext ec, EntityValue ev) {
        if (!requireInterfaceEntity()) return null
        String pkValue = ev.get(interfaceEntityPkField)
        return ec.entity.find(interfaceEntityName)
            .condition(interfaceEntityPkField, pkValue)
            .useCache(useCache).one()
    }

    private Map<String, Object> updateWithInterfaceEV(EntityValue ev, EntityFind efInterface) {
        Map<String, Object> jointOneMap, matchedOne
        jointOneMap = ev.getMap()
        if (efInterface != null) {
            matchedOne = efInterface.list().find({ ((EntityValue) it).get(interfaceEntityPkField).equals(ev.get(interfaceEntityPkField)) })
            jointOneMap.putAll(matchedOne)
        }
        return jointOneMap
    }

    private boolean requireInterfaceEntity() {
        return !(interfaceEntityName == null || interfaceEntityName.isEmpty() || entityName.equals(interfaceEntityName))
    }

    private EntityFind patchWithInCondition(EntityFind ef, DataFetchingEnvironment environment) {
        if (relKeyMap.size() != 1)
            throw new IllegalArgumentException("pathWithIdsCondition should only be used when there is just one relationship key map")
        int sourceItemCount = ((List) environment.source).size()
        String relParentFieldName, relFieldName
        List<Object> ids = new ArrayList<>(sourceItemCount)
        relParentFieldName = relKeyMap.keySet().asList().get(0)
        relFieldName = relKeyMap.values().asList().get(0)

        for (Object sourceItem in (List) environment.source) {
            Object relFieldValue = ((Map) sourceItem).get(relParentFieldName)
            if (relFieldValue != null) ids.add(relFieldValue)
        }

        ef.condition(relFieldName, ComparisonOperator.IN, ids)
        return ef
    }

    private EntityFind patchWithTupleOrCondition(EntityFind ef, DataFetchingEnvironment environment, ExecutionContext ec) {
        EntityCondition orCondition = null

        for (Object object in (List) environment.source) {
            EntityCondition tupleCondition = null
            Map sourceItem = (Map) object
            for (Map.Entry<String, String> entry in relKeyMap.entrySet()) {
                if (tupleCondition == null)
                    tupleCondition = ec.entity.conditionFactory.makeCondition(entry.getValue(), ComparisonOperator.EQUALS, sourceItem.get(entry.getKey()))
                else
                    tupleCondition = ec.entity.conditionFactory.makeCondition(tupleCondition, JoinOperator.AND,
                            ec.entity.conditionFactory.makeCondition(entry.getValue(), ComparisonOperator.EQUALS, sourceItem.get(entry.getKey())))
            }

            if (orCondition == null) orCondition = tupleCondition
            else orCondition = ec.entity.conditionFactory.makeCondition(orCondition, JoinOperator.OR, tupleCondition)
        }

        ef.condition(orCondition)
        return ef
    }

    private EntityFind patchWithConditions(EntityFind ef, DataFetchingEnvironment environment, ExecutionContext ec) {
        int relKeyCount = relKeyMap.size()
        if (relKeyCount == 1) {
            patchWithInCondition(ef, environment)
        } else if (relKeyCount > 1) {
            patchWithTupleOrCondition(ef, environment, ec)
        }
        return ef
    }

    private EntityFind patchFindOneWithConditions(EntityFind ef, Map sourceItem, ExecutionContext ec) {
        if (relKeyMap.size() == 0) return ef
        for (Map.Entry<String, String> entry in relKeyMap.entrySet()) {
            String relParentFieldName = entry.getKey()
            String relFieldName = entry.getValue()
            ef.condition(relFieldName, sourceItem.get(relParentFieldName))
        }
        return ef
    }

    @Override
    Object fetch(DataFetchingEnvironment environment) {
        logger.info("running batched data fetcher entity for entity [${entityName}] with operation [${operation}], use cache [${useCache}] ...")
//        logger.info("source     - ${environment.source}")
//        logger.info("arguments  - ${environment.arguments}")
//        logger.info("context    - ${environment.context}")
//        logger.info("fields     - ${environment.fields}")
//        logger.info("fieldType  - ${environment.fieldType}")
//        logger.info("parentType - ${environment.parentType}")
//        logger.info("schema     - ${environment.graphQLSchema}")
//        logger.info("relKeyMap  - ${relKeyMap}")
//        logger.info("interfaceEntityName    - ${interfaceEntityName}")

        ExecutionContext ec = ecf.getExecutionContext()

        int sourceItemCount = ((List) environment.source).size()
        int relKeyCount = relKeyMap.size()

        if (sourceItemCount == 0)
            throw new IllegalArgumentException("Source should be wrapped in List with at least 1 item for entity ${entityName}")

        if (sourceItemCount > 1 && relKeyCount == 0 && operation == "one")
            throw new IllegalArgumentException("Source contains more than 1 item, but no relationship key map defined for entity ${entityName}")

        List<String> actualLocalizedFields = DataFetcherUtils.getActualLocalizeFields(environment.fields[0].selectionSet, localizeFields, operation != "one")

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

            List<Map<String, Object>> resultList = new ArrayList<>((sourceItemCount != 0 ? sourceItemCount : 1) as int)
            for (int i = 0; i < sourceItemCount; i++) resultList.add(null)

            boolean requireInterfaceEF = requireInterfaceEntity()
            Map<String, Object> jointOneMap
            String cursor

            // When no relationship field, it means there is only one item in source and all conditions come from arguments.
            // When one relationship field, it means there are more than one item (including one) in source, and all conditions
            //          should include arguments and parent (source), Multiple one find operations could be combined as list
            //          operation with in condition, and finally decompose the result to corresponding item in source here.
            //          - But the list find still need to executed one by one
            // When more than one relationship field, the one find operations are still executed one by one separately. unless
            //          tuple condition can be made.
            //
            // For list operation, there are two issues to combine multiple into one
            //          - Each find may need to do pagination which must be separate
            //          - Even no pagination, how to apply limit equally to each actual find if they are combined into one.
            //
            if (operation == "one") {
                if (!useCache) {
                    EntityFind ef = ec.entity.find(entityName)
                            .searchFormMap(inputFieldsMap, null, null, null, false)
                            .useCache(useCache)
                    patchWithConditions(ef, environment, ec)

                    EntityList el = ef.list()
                    EntityFind efInterface = requireInterfaceEF ? getInterfaceEntityFind(ec, ef) : null

//                logger.info("---- branch batched data fetcher entity with ${((List) environment.source).size()} source for entity [${entityName}] with operation [${operation}] ----")
                    ((List) environment.source).eachWithIndex { Object object, int index ->
                        Map sourceItem = (Map) object
                        EntityValue evSelf = relKeyCount == 0 ? ef.one()
                                : el.find { EntityValue ev ->
                            int found = -1
                            for (Map.Entry<String, String> entry in relKeyMap.entrySet()) {
                                found = (found == -1) ? (sourceItem.get(entry.getKey()) == ev.get(entry.getValue()) ? 1 : 0)
                                        : (found == 1 && sourceItem.get(entry.getKey()) == ev.get(entry.getValue()) ? 1 : 0)
                            }
                            return found == 1
                        }
                        if (evSelf == null) return
                        jointOneMap = updateWithInterfaceEV(evSelf, efInterface)
                        cursor = GraphQLSchemaUtil.encodeRelayCursor(jointOneMap, pkFieldNames)
                        jointOneMap.put("id", cursor)
                        DataFetcherUtils.localize(jointOneMap, actualLocalizedFields, ec)
                        resultList.set(index, jointOneMap)
                    }

                    return resultList
                } else {
                    // When use cache, it is more efficient to iterate and get value from entity.record.one cache
                    ((List) environment.source).eachWithIndex{ Object object, int index ->
                        Map sourceItem = (Map) object
                        EntityFind ef = ec.entity.find(entityName)
                            .searchFormMap(inputFieldsMap, null, null, null, false)
                        patchFindOneWithConditions(ef, sourceItem, ec)
                        ef.useCache(useCache)

                        EntityValue evSelf = ef.one()
                        if (evSelf == null) return
                        EntityValue evInterface = requireInterfaceEF ? getInterfaceEntityValue(ec, evSelf) : null
                        jointOneMap = evSelf.getMap()
                        if (evInterface) jointOneMap.putAll(evInterface.getMap())
                        cursor = GraphQLSchemaUtil.encodeRelayCursor(jointOneMap, pkFieldNames)
                        jointOneMap.put("id", cursor)
                        DataFetcherUtils.localize(jointOneMap, actualLocalizedFields, ec)
                        resultList.set(index, jointOneMap)
                    }
                    return resultList
                }
            } else { // Operation == "list"
                Map<String, Object> resultMap
                Map<String, Object> edgesData
                List<Map<String, Object>> edgesDataList

                // No pagination needed pageInfo is not in the field selection set, so no need to construct it.
                if (!GraphQLSchemaUtil.requirePagination(environment)) {
//                    logger.info("---- branch batched data fetcher entity without pagination for entity [${entityName}] with operation [${operation}] ----")
                    inputFieldsMap.put("pageNoLimit", "true")
                    EntityFind ef = ec.entity.find(entityName)
                            .searchFormMap(inputFieldsMap, null, null, null, true)

                    GraphQLSchemaUtil.addPeriodValidArguments(ec, ef, environment.arguments)
                    patchWithConditions(ef, environment, ec)
                    ef.offset(null).limit(null).useCache(useCache)

                    EntityList el = ef.list()
                    EntityFind efInterface = requireInterfaceEF ? getInterfaceEntityFind(ec, ef) : null

                    ((List) environment.source).eachWithIndex { Object object, int index ->
                        Map sourceItem = (Map) object
                        EntityList elSource = relKeyCount == 0 ? el
                            : el.findAll { EntityValue ev ->
                                int found = -1
                                for (Map.Entry<String, String> entry in relKeyMap.entrySet()) {
                                    found = (found == -1) ? (sourceItem.get(entry.getKey()) == ev.get(entry.getValue()) ? 1 : 0)
                                            : (found == 1 && sourceItem.get(entry.getKey()) == ev.get(entry.getValue()) ? 1 : 0)
                                }
                                return found == 1
                            }
                        edgesDataList = elSource.collect { ev ->
                            edgesData = new HashMap<>(2)
                            cursor = GraphQLSchemaUtil.encodeRelayCursor(ev, pkFieldNames)
                            jointOneMap = updateWithInterfaceEV(ev, efInterface)

                            jointOneMap.put("id", cursor)
                            DataFetcherUtils.localize(jointOneMap, actualLocalizedFields, ec)
                            edgesData.put("cursor", cursor)
                            edgesData.put("node", jointOneMap)
                            return edgesData
                        }

                        resultMap = new HashMap<>(1)
                        resultMap.put("edges", edgesDataList)
                        resultList.set(index, resultMap)
                    }
                } else { // Used pagination or field selection set includes pageInfo
//                    logger.info("---- branch batched data fetcher entity with pagination for entity [${entityName}] with operation [${operation}] ----")
                    ((List) environment.source).eachWithIndex { Object object, int index ->
                        Map sourceItem = (Map) object
                        EntityFind ef = ec.entity.find(entityName)
                                .searchFormMap(inputFieldsMap, null, null, null, true)
                                .useCache(useCache)
                        GraphQLSchemaUtil.addPeriodValidArguments(ec, ef, environment.arguments)

                        for (Map.Entry<String, String> entry in relKeyMap.entrySet()) {
                            ef = ef.condition(entry.getValue(), sourceItem.get(entry.getKey()))
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

                        resultMap = new HashMap<>(2)
                        Map<String, Object> pageInfo = ['pageIndex'      : pageIndex, 'pageSize': pageSize, 'totalCount': count,
                                                        'pageMaxIndex'   : pageMaxIndex, 'pageRangeLow': pageRangeLow, 'pageRangeHigh': pageRangeHigh,
                                                        'hasPreviousPage': hasPreviousPage, 'hasNextPage': hasNextPage] as Map<String, Object>

                        EntityList el = ef.list()
                        edgesDataList = new ArrayList(el.size())

                        if (el != null && el.size() > 0) {
                            EntityFind efInterface = requireInterfaceEF ? getInterfaceEntityFind(ec, ef) : null

                            pageInfo.put("startCursor", GraphQLSchemaUtil.encodeRelayCursor(el.get(0), pkFieldNames))
                            pageInfo.put("endCursor", GraphQLSchemaUtil.encodeRelayCursor(el.get(el.size() - 1), pkFieldNames))
                            edgesDataList = el.collect { EntityValue ev ->
                                edgesData = new HashMap<>(2)
                                cursor = GraphQLSchemaUtil.encodeRelayCursor(ev, pkFieldNames)
                                jointOneMap = updateWithInterfaceEV(ev, efInterface)
                                jointOneMap.put("id", cursor)
                                DataFetcherUtils.localize(jointOneMap, actualLocalizedFields, ec)
                                edgesData.put("cursor", cursor)
                                edgesData.put("node", jointOneMap)
                                return edgesData
                            }
                        }
                        resultMap.put("edges", edgesDataList)
                        resultMap.put("pageInfo", pageInfo)
                        resultList.set(index, resultMap)
                    }
                }
                return resultList
            }
        }
        finally {
            if (loggedInAnonymous) ((UserFacadeImpl) ec.getUser()).logoutAnonymousOnly()
        }
        return null
    }
}
