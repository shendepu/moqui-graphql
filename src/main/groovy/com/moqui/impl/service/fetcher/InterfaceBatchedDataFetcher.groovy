package com.moqui.impl.service.fetcher

import com.moqui.impl.util.GraphQLSchemaUtil
import graphql.execution.batched.BatchedDataFetcher
import graphql.language.Field
import graphql.schema.DataFetchingEnvironment
import org.moqui.context.ExecutionContext
import org.moqui.context.ExecutionContextFactory
import org.moqui.entity.EntityCondition
import org.moqui.entity.EntityCondition.ComparisonOperator
import org.moqui.entity.EntityCondition.JoinOperator
import org.moqui.entity.EntityFind
import org.moqui.impl.context.UserFacadeImpl
import org.moqui.util.MNode

import org.slf4j.Logger
import org.slf4j.LoggerFactory


import static com.moqui.impl.service.GraphQLSchemaDefinition.FieldDefinition

class InterfaceBatchedDataFetcher extends BaseDataFetcher implements BatchedDataFetcher {
    protected final static Logger logger = LoggerFactory.getLogger(InterfaceBatchedDataFetcher.class)

    String primaryField
    String resolverField
    String requireAuthentication
    String operation
    String fieldRawType
    InternalDataFetcher defaultFetcher
    Map<String, String> relKeyMap = new HashMap<>(1)
    Map<String, InternalDataFetcher> resolverFetcherMap = new HashMap<>(1)

    InterfaceBatchedDataFetcher(MNode node, MNode refNode, FieldDefinition fieldDef, ExecutionContextFactory ecf) {
        super(fieldDef, ecf)

        primaryField = node.attribute("primary-field") ?: (refNode != null ? refNode.attribute("primary-field") : "")
        resolverField = node.attribute("resolver-field") ?: (refNode != null ? refNode.attribute("resolver-field") : "")

        Map<String, String> pkRelMap = new HashMap<>(1)
        pkRelMap.put(primaryField, primaryField)

        ArrayList<MNode> keyMapChildren = node.children("key-map") ?: refNode?.children("key-map")
        for (MNode keyMapNode in keyMapChildren) {
            relKeyMap.put(keyMapNode.attribute("field-name"), keyMapNode.attribute("related") ?: keyMapNode.attribute("field-name"))
        }

        ArrayList<MNode> defaultFetcherChildren = node.children("default-fetcher") ?: refNode?.children("default-fetcher")

        if (defaultFetcherChildren.size() != 1) throw new IllegalArgumentException("interface-fetcher.default-fetcher not found")
        MNode defaultFetcherNode = defaultFetcherChildren[0]
        defaultFetcher = buildDataFetcher(defaultFetcherNode.children[0], fieldDef, ecf, relKeyMap)

        ArrayList<MNode> resolverFetcherChildren = node.children("resolver-fetcher") ?: refNode?.children("resolver-fetcher")
        for (MNode resolverFetcherNode in resolverFetcherChildren) {
            String resolverValue = resolverFetcherNode.attribute("resolver-value")
            InternalDataFetcher dataFetcher = buildDataFetcher(resolverFetcherNode.children[0], fieldDef, ecf, pkRelMap)
            resolverFetcherMap.put(resolverValue, dataFetcher)
        }

        initializeFields()
    }

    private void initializeFields() {
        this.requireAuthentication = fieldDef.requireAuthentication ?: "true"
        this.fieldRawType = fieldDef.type
        if ("true".equals(fieldDef.isList)) this.operation = "list"
        else this.operation = "one"
    }

    private static InternalDataFetcher buildDataFetcher(MNode node, FieldDefinition fieldDef, ExecutionContextFactory ecf, Map<String, String> relKeyMap) {
        switch (node.name) {
            case "entity-fetcher":
                return new InternalEntityDataFetcher(node, fieldDef, ecf, relKeyMap)
                break
            case "service-fetcher":
                return new InternalServiceDataFetcher(node, fieldDef, ecf, relKeyMap)
                break
        }
    }

    private static boolean requirePagination(DataFetchingEnvironment environment) {
        List sources = (List) environment.source

        Map<String, Object> arguments = (Map) environment.arguments
        List<Field> fields = (List) environment.fields
        boolean result = false
        result = result || arguments.get("pagination") != null
        if (result) return true
        result = result || fields.find({ it.name == "pageInfo" }) != null
        if (!result) result = sources.size() == 1

        return result
    }

    private List<Map<String, Object>> mergeWithConcreteValue(List<Map<String, Object>> interfaceValueList) {
        Set<String> resolverValues = new HashSet<>()

        interfaceValueList.each { Map<String, Object> it ->
            resolverValues.add(it.get(resolverField) as String)
        }

        for (String resolverValue in resolverValues) {
            InternalDataFetcher resolverFetcher = resolverFetcherMap.get(resolverValue)
            if (resolverFetcher == null) continue

            List<Map<String, Object>> filterValueList = interfaceValueList.findAll { Map<String, Object> it ->
                it.get(resolverField) == resolverValue
            }

            List<Map<String, Object>> concreteValueList = resolverFetcher.searchFormMap(filterValueList, [:])

            concreteValueList.each { Map<String, Object> concreteValue ->
                Map<String, Object> interValue = interfaceValueList.find { Map<String, Object> it ->
                    it.get(primaryField) == concreteValue.get(primaryField)
                }
                if (interValue != null) interValue.putAll(concreteValue)
            }
        }
        return interfaceValueList
    }

    private static boolean matchParentByRelKeyMap(Map<String, Object> sourceItem, Map<String, Object> self, Map<String, String> relKeyMap) {
        int found = -1
        for (Map.Entry<String, String> entry in relKeyMap.entrySet()) {
            found = (found == -1) ? (sourceItem.get(entry.key) == self.get(entry.value) ? 1 : 0)
                    : (found == 1 && sourceItem.get(entry.key) == self.get(entry.value) ? 1 : 0)
        }
        return found == 1
    }

    @Override
    Object fetch(DataFetchingEnvironment environment) {
        logger.info("---- running interface data fetcher with operation [${operation}] ...")
//        logger.info("source     - ${environment.source}")
//        logger.info("arguments  - ${environment.arguments}")
//        logger.info("context    - ${environment.context}")
//        logger.info("fields     - ${environment.fields}")
//        logger.info("fieldType  - ${environment.fieldType}")
//        logger.info("parentType - ${environment.parentType}")
//        logger.info("relKeyMap  - ${relKeyMap}")

        ExecutionContext ec = ecf.getExecutionContext()

        int sourceItemCount = ((List) environment.source).size()
        int relKeyCount = relKeyMap.size()

        if (sourceItemCount == 0)
            throw new IllegalArgumentException("Source should be wrapped in List with at least 1 item")

        if (sourceItemCount > 1 && relKeyCount == 0 && operation == "one")
            throw new IllegalArgumentException("Source contains more than 1 item, but no relationship key map defined")

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

            if (operation == "one") {
//                logger.warn("running one operation")
                List<Map<String, Object>> interfaceValueList = defaultFetcher.searchFormMap((List) environment.source, inputFieldsMap)

                mergeWithConcreteValue(interfaceValueList)

                ((List) environment.source).eachWithIndex{ Object object, int index ->
                    Map sourceItem = (Map) object

                    jointOneMap = relKeyCount == 0 ? (interfaceValueList.size() > 0 ? interfaceValueList[0] : null) :
                        interfaceValueList.find { Map<String, Object> it -> matchParentByRelKeyMap(sourceItem, it, relKeyMap) }
                    resultList.set(index, jointOneMap)
                }
            } else { // Operation == "list"
                Map<String, Object> resultMap
                Map<String, Object> edgesData
                String cursor
                List<Map<String, Object>> edgesDataList

                if (!requirePagination(environment)) {
//                    logger.warn("running list with batch")
                    inputFieldsMap.put("noPageLimit", "true")
                    List<Map<String, Object>> interfaceValueList = defaultFetcher.searchFormMap((List) environment.source, inputFieldsMap)

                    mergeWithConcreteValue(interfaceValueList)

                    ((List) environment.source).eachWithIndex{ Object object, int index ->
                        Map sourceItem = (Map) object
                        List<Map<String, Object>> jointOneList = relKeyCount == 0 ? interfaceValueList :
                            interfaceValueList.findAll { Map<String, Object> it -> matchParentByRelKeyMap(sourceItem, it, relKeyMap) }


                        edgesDataList = jointOneList.collect { Map<String, Object> it ->
                            edgesData = new HashMap<>(2)
                            cursor = GraphQLSchemaUtil.base64EncodeCursor(it, fieldRawType, [primaryField])
                            edgesData.put("cursor", cursor)
                            edgesData.put("node", it)
                            return edgesData
                        }
                        resultMap = new HashMap<>(1)
                        resultMap.put("edges", edgesDataList)
                        resultList.set(index, resultMap)
                    }
                } else { // Used pagination or field selection set includes pageInfo
//                    logger.warn("running list with no batch!!!!")
                    ((List) environment.source).eachWithIndex { Object object, int index ->
                        Map sourceItem = (Map) object

                        Map<String, Object> interfaceValueMap = defaultFetcher.searchFormMapWithPagination([sourceItem], inputFieldsMap)

                        List<Map<String, Object>> interfaceValueList = mergeWithConcreteValue(interfaceValueMap.data as List<Map<String, Object>>)

                        int count = interfaceValueMap.count as int
                        int pageIndex = interfaceValueMap.pageIndex as int
                        int pageSize = interfaceValueMap.pageSize as int
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

                        edgesDataList = new ArrayList(interfaceValueList.size())

                        if (interfaceValueList != null && interfaceValueList.size() > 0) {
                            pageInfo.put("startCursor", GraphQLSchemaUtil.base64EncodeCursor(interfaceValueList.get(0), fieldRawType, [primaryField]))
                            pageInfo.put("endCursor", GraphQLSchemaUtil.base64EncodeCursor(interfaceValueList.get(interfaceValueList.size() - 1), fieldRawType, [primaryField]))
                            edgesDataList = interfaceValueList.collect { Map<String, Object> it ->
                                edgesData = new HashMap<>(2)
                                cursor = GraphQLSchemaUtil.base64EncodeCursor(it, fieldRawType, [primaryField])
                                edgesData.put("cursor", cursor)
                                edgesData.put("node", it)
                                return edgesData
                            }
                        }
                        resultMap.put("edges", edgesDataList)
                        resultMap.put("pageInfo", pageInfo)
                        resultList.set(index, resultMap)
                    }
                }
            }

            return resultList
        }
        finally {
            if (loggedInAnonymous) ((UserFacadeImpl) ec.getUser()).logoutAnonymousOnly()
        }
        return null
    }

    static abstract class InternalDataFetcher {
        FieldDefinition fieldDef
        ExecutionContextFactory ecf
        Map<String, String> relKeyMap = new HashMap<>(1)

        InternalDataFetcher(FieldDefinition fieldDef, ExecutionContextFactory ecf, Map<String, String> relKeyMap) {
            this.fieldDef = fieldDef
            this.ecf = ecf
            this.relKeyMap.putAll(relKeyMap)
        }

        abstract List<Map<String, Object>> searchFormMap(List<Object> source, Map<String, Object> inputFieldsMap)
        abstract Map<String, Object> searchFormMapWithPagination(List<Object> source, Map<String, Object> inputFieldsMap)

//        abstract List<Map<String, Object>> getByPkField(String pkField, List<String> pkValues)
    }

    static class InternalEntityDataFetcher extends InternalDataFetcher {
        String entityName

        InternalEntityDataFetcher(MNode node, FieldDefinition fieldDef, ExecutionContextFactory ecf, Map<String, String> relKeyMap) {
            super(fieldDef, ecf, relKeyMap)

            entityName = node.attribute("entity-name")
        }

        @Override
        List<Map<String, Object>> searchFormMap(List<Object> source, Map<String, Object> inputFieldsMap) {
            ExecutionContext ec = ecf.getExecutionContext()
            EntityFind ef = ec.entity.find(entityName).searchFormMap(inputFieldsMap, null, null, null, false)

            patchWithConditions(ef, source, ec)

            return ef.list().getPlainValueList(0)
        }

        @Override
        Map<String, Object> searchFormMapWithPagination(List<Object> source, Map<String, Object> inputFieldsMap) {
            ExecutionContext ec = ecf.getExecutionContext()
            EntityFind ef = ec.entity.find(entityName).searchFormMap(inputFieldsMap, null, null, null, false)
            patchWithConditions(ef, source, ec)

            if (!ef.getLimit()) ef.limit(100)

            Map<String, Object> resultMap = new HashMap<>()

            resultMap.put("pageIndex", ef.getPageIndex())
            resultMap.put("pageSize", ef.getPageSize())
            resultMap.put("count", ef.count())
            resultMap.put("data", ef.list().getPlainValueList(0))
            return resultMap
        }

        private EntityFind patchWithInCondition(EntityFind ef, List<Object> source) {
            if (relKeyMap.size() != 1)
                throw new IllegalArgumentException("pathWithIdsCondition should only be used when there is just one relationship key map")
            int sourceItemCount = source.size()
            String relParentFieldName, relFieldName
            List<Object> ids = new ArrayList<>(sourceItemCount)
            relParentFieldName = relKeyMap.keySet().asList().get(0)
            relFieldName = relKeyMap.values().asList().get(0)

            for (Object sourceItem in source) {
                Object relFieldValue = ((Map) sourceItem).get(relParentFieldName)
                if (relFieldValue != null) ids.add(relFieldValue)
            }

            ef.condition(relFieldName, ComparisonOperator.IN, ids)
            return ef
        }

        private EntityFind patchWithTupleOrCondition(EntityFind ef, List<Object> source, ExecutionContext ec) {
            EntityCondition orCondition = null

            for (Object object in source) {
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

        private EntityFind patchWithConditions(EntityFind ef, List<Object> source, ExecutionContext ec) {
            int relKeyCount = relKeyMap.size()
            if (relKeyCount == 1) {
                patchWithInCondition(ef, source)
            } else if (relKeyCount > 1) {
                patchWithTupleOrCondition(ef, source, ec)
            }
            return ef
        }

    }

    static class InternalServiceDataFetcher extends InternalDataFetcher {
        InternalServiceDataFetcher(MNode node, FieldDefinition fieldDef, ExecutionContextFactory ecf, Map<String, String> relKeyMap) {
            super(fieldDef, ecf, relKeyMap)
        }

        @Override
        List<Map<String, Object>> searchFormMap(List<Object> source, Map<String, Object> inputFieldsMap) {

        }

        @Override
        Map<String, Object> searchFormMapWithPagination(List<Object> source, Map<String, Object> inputFieldsMap) {
            return null
        }
    }
}
