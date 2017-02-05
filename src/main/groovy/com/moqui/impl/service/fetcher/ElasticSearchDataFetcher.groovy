package com.moqui.impl.service.fetcher

import graphql.schema.DataFetchingEnvironment
import org.moqui.context.ExecutionContext
import org.moqui.context.ExecutionContextFactory
import org.moqui.entity.EntityException
import org.moqui.entity.EntityValue
import org.moqui.impl.context.UserFacadeImpl
import org.moqui.util.MNode
import org.elasticsearch.client.Client
import org.elasticsearch.action.get.GetResponse

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static com.moqui.impl.service.GraphQLSchemaDefinition.FieldDefinition

class ElasticSearchDataFetcher extends BaseDataFetcher {
    protected final static Logger logger = LoggerFactory.getLogger(ElasticSearchDataFetcher.class)

    String dataDocumentId
    String indexName
    String requireAuthentication
    List<String> localizeFields = new ArrayList<>()

    ElasticSearchDataFetcher(MNode node, FieldDefinition fieldDef, ExecutionContextFactory ecf) {
        super(fieldDef, ecf)
        this.requireAuthentication = node.attribute("require-authentication") ?: fieldDef.requireAuthentication ?: "true"

        dataDocumentId = node.attribute("data-document-id")

        for (MNode localizeFieldNode in node.children("localize-field")) {
            localizeFields.add(localizeFieldNode.attribute("name"))
        }

        boolean alreadyDisabled = ecf.executionContext.artifactExecution.disableAuthz()
        try {
            EntityValue dataDocument = ecf.entity.find("moqui.entity.document.DataDocument").condition("dataDocumentId", dataDocumentId).one()
            if (dataDocument == null) throw new EntityException("Can't find data document ${dataDocumentId}")
            this.indexName = dataDocument.get("indexName")
        } finally {
            if (!alreadyDisabled) ecf.executionContext.artifactExecution.enableAuthz()
        }
    }

    private static localizeFieldPath(ExecutionContext ec, Object parentData, String restPathElement) {
        if (parentData == null) return
        while (!restPathElement.isEmpty()) {
            if (parentData instanceof List) {
                for (Object listItem in parentData) localizeFieldPath(ec, listItem, restPathElement)
            } else if (parentData instanceof Map) {
                String currentPathElement
                int colonPos = restPathElement.indexOf(":")
                if (colonPos > -1) {
                    currentPathElement = restPathElement.substring(0, colonPos)
                    restPathElement = restPathElement.substring(colonPos + 1)
                } else {
                    currentPathElement = restPathElement
                    restPathElement = ""
                }

                Object currentData = parentData.get(currentPathElement)
                if (currentData == null) return

                if (currentData instanceof List || currentData instanceof Map) {
                    localizeFieldPath(ec, currentData, restPathElement)
                } else if (currentData instanceof String) {
                    if (!restPathElement.isEmpty()) {
                        logger.warn("Localize field of data document is not on leaf")
                        return
                    }
                    parentData.put(currentPathElement, ec.l10n.localize(currentData))
                }
            } else {
                return // not list, map nested structure
            }
        }
    }

    private void localizeDocument(Map<String, Object> document) {
        if (document == null) return
        for (String localizeField in localizeFields) {
            localizeFieldPath(ecf.getExecutionContext(), document, localizeField)
        }
    }

    private static void populateResult(Map<String, Object> dataMap, Map<String, Object> dataTreeCurrent) {
        if (dataTreeCurrent == null) return
        for (Map.Entry<String, Object> entry in dataTreeCurrent) {
            if (entry.value instanceof List) {
                List<Map<String, Object>> dataTreeChildList = entry.value as List
                List<Map<String, Object>> edgesDataList = new ArrayList<>(dataTreeChildList.size())
                for (Map<String, Object> dataTreeChild in dataTreeChildList) {
                    Map<String, Object> edgeMap = new HashMap<>(2)
                    Map<String, Object> nodeMap = new HashMap<>()
                    populateResult(nodeMap, dataTreeChild)
                    edgeMap.put("node", nodeMap)
                    // no cursor
                    edgesDataList.add(edgeMap)
                }
                dataMap.put("edges", edgesDataList)
                // no pageInfo
            } else if (entry.value instanceof Map) {
                Map<String, Object> dataTreeChild = entry.value as Map
                Map<String, Object> dataChildMap = new HashMap<>()
                populateResult(dataChildMap, dataTreeChild)
                dataMap.put(entry.key, dataChildMap)
            } else {
                dataMap.put(entry.key, entry.value)
            }
        }
    }

    @Override
    Object fetch(DataFetchingEnvironment environment) {
        logger.info("---- running data fetcher elastic search on index [${indexName}] and document type A${dataDocumentId} ...")

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

            if (fieldDef.isList == "true") {
                Map<String, Object> paramMap = [indexName  : indexName, documentType: dataDocumentId, flattenDocument: false,
                                                queryString: environment.arguments.get("queryString")]

                Map<String, Object> ddMap = ec.service.sync().name("org.moqui.search.SearchServices.search#DataDocuments").parameters(paramMap).call()

                int pageIndex = ddMap.documentListPageIndex as Integer
                int pageSize = ddMap.documentListPageSize as Integer
                int totalCount = ddMap.documentListCount as Integer
                int pageMaxIndex = ddMap.documentListPageMaxIndex as Integer
                int pageRangeLow = ddMap.documentListPageRangeLow as Integer
                int pageRangeHigh = ddMap.documentListPageRangeHigh as Integer
                boolean hasPreviousPage = pageIndex > 0
                boolean hasNextPage = pageMaxIndex > pageIndex


                Map<String, Object> resultMap = new HashMap<>(2)
                Map<String, Object> pageInfo = ['pageIndex'      : pageIndex, 'pageSize': pageSize, 'totalCount': totalCount,
                                                'pageMaxIndex'   : pageMaxIndex, 'pageRangeLow': pageRangeLow, 'pageRangeHigh': pageRangeHigh,
                                                'hasPreviousPage': hasPreviousPage, 'hasNextPage': hasNextPage] as Map<String, Object>

                List<Map<String, Object>> documentList = ddMap.documentList as List
                List<Map<String, Object>> edgesDataList = new ArrayList<>(documentList.size())
                for (Map<String, Object> document in documentList) {
                    localizeDocument(document)
                    Map<String, Object> edgeMap = new HashMap<>(2)
                    Map<String, Object> nodeMap = new HashMap<>()
                    populateResult(nodeMap, document)
                    edgeMap.put("node", nodeMap)
                    // no cursor
                    edgesDataList.add(edgeMap)
                }
                resultMap.put("edges", edgesDataList)
                resultMap.put("pageInfo", pageInfo)
                return resultMap
            } else {
                String _id = environment.arguments.get("_id")

                Client elasticSearchClient = ec.getTool("ElasticSearch", Client.class)
                GetResponse docGr = elasticSearchClient.prepareGet(indexName, dataDocumentId, _id).execute().actionGet()

                Map<String, Object> resultMap = [:]
                Map<String, Object> document = docGr.getSourceAsMap()
                localizeDocument(document)

                populateResult(resultMap, document)
                return resultMap
            }

            return null
        } finally {
            if (loggedInAnonymous) ((UserFacadeImpl) ec.getUser()).logoutAnonymousOnly()
        }
    }
}
