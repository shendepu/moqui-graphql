package com.moqui.impl.service.fetcher

import com.moqui.impl.util.GraphQLSchemaUtil
import graphql.execution.batched.BatchedDataFetcher
import graphql.schema.DataFetchingEnvironment
import groovy.transform.CompileStatic
import org.moqui.context.ExecutionContext
import org.moqui.context.ExecutionContextFactory
import org.moqui.entity.EntityCondition.ComparisonOperator
import org.moqui.entity.EntityFind
import org.moqui.impl.context.UserFacadeImpl
import org.moqui.util.MNode

import static com.moqui.impl.service.GraphQLSchemaDefinition.FieldDefinition

@CompileStatic
class EntityBatchedDataFetcher extends BaseEntityDataFetcher implements BatchedDataFetcher {
    private boolean interfaceRequired
    
    EntityBatchedDataFetcher(MNode node, FieldDefinition fieldDef, ExecutionContextFactory ecf) {
        super(node, fieldDef, ecf)
        interfaceRequired = requireInterface()

    }

    EntityBatchedDataFetcher(ExecutionContextFactory ecf, FieldDefinition fieldDef, String entityName, Map<String, String> relKeyMap) {
        this(ecf, fieldDef, entityName, null, relKeyMap)
    }

    EntityBatchedDataFetcher(ExecutionContextFactory ecf, FieldDefinition fieldDef, String entityName, String interfaceEntityName, Map<String, String> relKeyMap) {
        super(ecf, fieldDef, entityName, interfaceEntityName, relKeyMap)
        interfaceRequired = requireInterface()
    }

    private boolean requireInterface () {
        return !(interfaceEntityName == null || interfaceEntityName.isEmpty() || entityName.equals(interfaceEntityName))
    }

    private List<Map<String, Object>> mergeWithInterfaceValue(ExecutionContext ec, List<Map<String, Object>> concreteValueList) {
        if (!interfaceRequired) return concreteValueList

        List<Object> pkValues = new ArrayList<>()
        for (Map concreteValue in concreteValueList) pkValues.add(concreteValue.get(interfaceEntityPkField))
        List<Map<String, Object>> interfaceValueList = ec.entity.find(interfaceEntityName).useCache(useCache)
                .condition(interfaceEntityPkField, ComparisonOperator.IN, pkValues.toSet().sort())
                .list().getValueMapList()
        
        interfaceValueList = interfaceValueList.collect({ Map<String, Object> interfaceValue ->
            Map<String, Object> concreteValue = concreteValueList.find({ Map<String, Object> it -> it.get(interfaceEntityPkField) == interfaceValue.get(interfaceEntityPkField)})
            if (concreteValue) interfaceValue.putAll(concreteValue)
            return interfaceValue
        })
        
        return interfaceValueList
    }

    private Map<String, Object> mergeWithInterfaceValue(ExecutionContext ec, Map<String, Object> concreteValue) {
        if (!interfaceRequired) return concreteValue
        Map jointOneMap = ec.entity.find(interfaceEntityName).useCache(useCache)
            .condition(interfaceEntityPkField, concreteValue.get(interfaceEntityPkField))
            .one()?.getMap()
        if (jointOneMap != null) jointOneMap.putAll(concreteValue)
        return jointOneMap
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
                List<Map<String, Object>> jointValueList
                if (!useCache) {
                    EntityFind efConcrete = ec.entity.find(entityName)
                            .useCache(useCache)
                            .searchFormMap(inputFieldsMap, null, null, null, false)

                    DataFetcherUtils.patchWithConditions(efConcrete, environment.source as List, relKeyMap, ec)
                    jointValueList = mergeWithInterfaceValue(ec, efConcrete.list().getValueMapList())

                } else {
                    jointValueList = new ArrayList<>(sourceItemCount)
                    ((List) environment.source).eachWithIndex { Object object, int index ->
                        Map sourceItem = (Map) object
                        EntityFind efConcrete = ec.entity.find(entityName).useCache(useCache)
                                .searchFormMap(inputFieldsMap, null, null, null, false)
                        DataFetcherUtils.patchFindOneWithConditions(efConcrete, sourceItem, relKeyMap, ec)
                        jointOneMap = mergeWithInterfaceValue(ec, efConcrete.one()?.getMap())
                        if (jointOneMap) jointValueList.add(jointOneMap)
                    }
                }

                ((List) environment.source).eachWithIndex { Object object, int index ->
                    Map sourceItem = (Map) object

                    jointOneMap = (relKeyCount == 0 ? (jointValueList.size() > 0 ? jointValueList[0] : null) :
                            jointValueList.find { Object it -> DataFetcherUtils.matchParentByRelKeyMap(sourceItem, it as Map<String, Object>, relKeyMap) }) as Map<String, Object>

                    if (jointOneMap == null) return
                    cursor = GraphQLSchemaUtil.encodeRelayCursor(jointOneMap, pkFieldNames)
                    jointOneMap.put("id", cursor)
                    DataFetcherUtils.localize(jointOneMap, actualLocalizedFields, ec)
                    resultList.set(index, jointOneMap)
                }
                return resultList
            } else { // Operation == "list"
                Map<String, Object> resultMap
                Map<String, Object> edgesData
                List<Map<String, Object>> edgesDataList

                // No pagination needed pageInfo is not in the field selection set, so no need to construct it.
                if (!GraphQLSchemaUtil.requirePagination(environment)) {
//                    logger.info("---- not require pagination ----")
                    inputFieldsMap.put("pageNoLimit", "true")
                    EntityFind efConcrete = ec.entity.find(entityName)
                            .useCache(useCache)
                            .searchFormMap(inputFieldsMap, null, null, null, true)

                    GraphQLSchemaUtil.addPeriodValidArguments(ec, efConcrete, environment.arguments)
                    DataFetcherUtils.patchWithConditions(efConcrete, environment.source as List, relKeyMap, ec)

                    List<Map<String, Object>> jointValueList = mergeWithInterfaceValue(ec, efConcrete.list().getValueMapList())

                    ((List) environment.source).eachWithIndex { Object object, int index ->
                        Map sourceItem = (Map) object
                        List<Map<String, Object>> matchedJointValueList
                        if (relKeyCount == 0) {
                            matchedJointValueList = jointValueList
                        } else {
                            matchedJointValueList = jointValueList.findAll { Object it -> DataFetcherUtils.matchParentByRelKeyMap(sourceItem, it as Map<String, Object>, relKeyMap) }

                        }
                        edgesDataList = matchedJointValueList.collect { Object it ->
                            Map<String, Object> matchedJointOneMap = it as Map<String, Object>
                            edgesData = new HashMap<>(2)
                            cursor = GraphQLSchemaUtil.encodeRelayCursor(matchedJointOneMap, pkFieldNames)

                            matchedJointOneMap.put("id", cursor)
                            DataFetcherUtils.localize(matchedJointOneMap, actualLocalizedFields, ec)
                            edgesData.put("cursor", cursor)
                            edgesData.put("node", matchedJointOneMap)
                            return edgesData
                        }

                        resultMap = new HashMap<>(1)
                        resultMap.put("edges", edgesDataList)
                        resultList.set(index, resultMap)
                    }
                } else { // Used pagination or field selection set includes pageInfo
//                    logger.info("---- require pagination ----")
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

                        List<Map<String, Object>> jointValueList = mergeWithInterfaceValue(ec, ef.list().getValueMapList())

                        edgesDataList = new ArrayList(0)

                        if (jointValueList != null && jointValueList.size() > 0) {
                            pageInfo.put("startCursor", GraphQLSchemaUtil.encodeRelayCursor(jointValueList.get(0), pkFieldNames))
                            pageInfo.put("endCursor", GraphQLSchemaUtil.encodeRelayCursor(jointValueList.get(jointValueList.size() - 1), pkFieldNames))
                            edgesDataList = jointValueList.collect { Map<String, Object> it ->
                                jointOneMap = it
                                edgesData = new HashMap<>(2)
                                cursor = GraphQLSchemaUtil.encodeRelayCursor(jointOneMap, pkFieldNames)
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
