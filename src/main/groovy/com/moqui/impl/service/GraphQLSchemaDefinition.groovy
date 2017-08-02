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
package com.moqui.impl.service

import com.moqui.impl.service.fetcher.BaseDataFetcher
import com.moqui.impl.service.fetcher.ElasticSearchDataFetcher
import com.moqui.impl.service.fetcher.EmptyDataFetcher
import com.moqui.impl.service.fetcher.EntityBatchedDataFetcher
import com.moqui.impl.service.fetcher.InterfaceBatchedDataFetcher
import com.moqui.impl.service.fetcher.ServiceDataFetcher
import com.moqui.impl.util.GraphQLSchemaUtil
import graphql.schema.GraphQLArgument
import graphql.schema.GraphQLEnumType
import graphql.schema.GraphQLFieldDefinition
import graphql.schema.GraphQLInputObjectField
import graphql.schema.GraphQLInputObjectType
import graphql.schema.GraphQLInputType
import graphql.schema.GraphQLInterfaceType
import graphql.schema.GraphQLList
import graphql.schema.GraphQLNonNull
import graphql.schema.GraphQLObjectType
import graphql.schema.GraphQLOutputType
import graphql.schema.GraphQLScalarType
import graphql.schema.GraphQLSchema
import graphql.schema.GraphQLType
import graphql.schema.GraphQLTypeReference
import graphql.schema.GraphQLUnionType
import graphql.schema.SchemaUtil
import graphql.schema.TypeResolver
import groovy.transform.CompileStatic
import org.moqui.context.ExecutionContextFactory
import org.moqui.impl.context.ExecutionContextFactoryImpl
import org.moqui.impl.entity.EntityDefinition
import org.moqui.impl.entity.FieldInfo
import org.moqui.impl.service.ServiceDefinition
import org.moqui.util.MNode
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.lang.reflect.Method

import static graphql.Scalars.GraphQLBoolean
import static graphql.Scalars.GraphQLChar
import static graphql.Scalars.GraphQLInt
import static graphql.Scalars.GraphQLString

@CompileStatic
class GraphQLSchemaDefinition {
    protected final static Logger logger = LoggerFactory.getLogger(GraphQLSchemaDefinition.class)

    @SuppressWarnings("GrFinalVariableAccess")
    protected final ExecutionContextFactory ecf

    @SuppressWarnings("GrFinalVariableAccess")
    protected final Map<String, MNode> schemaNodeMap

    protected final String queryRootObjectTypeName = "QueryRootObjectType"
    protected final String mutationRootObjectTypeName = "MutationRootObjectType"

    protected final Map<String, String> queryRootFieldMap = new LinkedHashMap<>()
    protected final Map<String, String> mutationRootFieldMap = new LinkedHashMap<>()


    protected static final Map<String, GraphQLOutputType> graphQLOutputTypeMap = new HashMap<>()
    protected static final Map<String, GraphQLInputType> graphQLInputTypeMap = new HashMap<>()
    protected static final Map<String, GraphQLEnumType> graphQLEnumTypeMap = new HashMap<>()
    protected static final Map<String, GraphQLUnionType> graphQLUnionTypeMap = new HashMap<>()
    protected static final Map<String, GraphQLInterfaceType> graphQLInterfaceTypeMap = new HashMap<>()
    protected static final Map<String, GraphQLObjectType> graphQLObjectTypeMap = new HashMap<>()
    protected static final Map<String, GraphQLFieldDefinition> graphQLFieldMap = new HashMap<>()
    protected static final Map<String, GraphQLInputObjectType> graphQLInputObjectTypeMap = new HashMap<>()
    protected static final Map<String, GraphQLInputObjectField> graphQLInputObjectFieldMap = new HashMap<>()
    protected static final Map<String, GraphQLArgument> graphQLArgumentMap = new HashMap<>()
    protected static final Map<String, GraphQLArgument> graphQLDirectiveArgumentMap = new LinkedHashMap<>()
    protected static final Map<String, GraphQLTypeReference> graphQLTypeReferenceMap = new HashMap<>()

    protected final Map<String, GraphQLInputType> schemaInputTypeMap = new HashMap<>()

    protected final ArrayList<String> schemaInputTypeNameList = new ArrayList<>()

    protected Map<String, GraphQLTypeDefinition> allTypeDefMap = new LinkedHashMap<>()
    protected LinkedList<GraphQLTypeDefinition> allTypeDefSortedList = new LinkedList<>()
    protected Map<String, GraphQLTypeDefinition> requiredTypeDefMap = new LinkedHashMap<>()

    protected static Map<String, MNode> interfaceFetcherNodeMap = new HashMap<>()
    protected static Set<String> interfaceResolverTypeSet = new HashSet<>()

    protected static Map<String, FieldDefinition> fieldDefMap = new HashMap<>()

    protected static Map<String, ArgumentDefinition> argumentDefMap = new HashMap<>()

    protected Map<String, InterfaceTypeDefinition> interfaceTypeDefMap = new LinkedHashMap<>()
    protected Map<String, ExtendObjectDefinition> extendObjectDefMap = new LinkedHashMap<>()

    protected static GraphQLObjectType pageInfoType
    protected static GraphQLInputObjectType paginationInputType
    protected static GraphQLInputObjectType operationInputType
    protected static GraphQLInputObjectType dateRangeInputType
    protected static GraphQLFieldDefinition cursorField
    protected static GraphQLFieldDefinition clientMutationIdField
    protected static GraphQLArgument paginationArgument
    protected static GraphQLArgument ifArgument
    protected static GraphQLInputObjectField clientMutationIdInputField

    protected static final String KEY_SPLITTER = "__"
    protected static final String NON_NULL_SUFFIX = "_1"
    protected static final String IS_LIST_SUFFIX = "_2"
    protected static final String LIST_ITEM_NON_NULL_SUFFIX = "_3"
    protected static final String REQUIRED_SUFFIX = "_a"

    static {
        createPredefinedGraphQLTypes()
    }

    private static void createPredefinedGraphQLTypes() {
        // Add default GraphQLScalarType
        for (Map.Entry<String, GraphQLScalarType> entry in GraphQLSchemaUtil.graphQLScalarTypes.entrySet()) {
            graphQLInputTypeMap.put(entry.getKey(), entry.getValue())
            graphQLOutputTypeMap.put(entry.getKey(), entry.getValue())
        }

        // Predefined GraphQLFieldDefinition
        GraphQLFieldDefinition.Builder cursorFieldBuilder = GraphQLFieldDefinition.newFieldDefinition().name("cursor")
                .type(GraphQLString)
        for (Map.Entry<String, GraphQLArgument> entry in graphQLDirectiveArgumentMap) cursorFieldBuilder.argument(entry.getValue())
        cursorField = cursorFieldBuilder.build()
        graphQLFieldMap.put("cursor" + KEY_SPLITTER + "String", cursorField)

        GraphQLFieldDefinition.Builder clientMutationIdFieldBuilder = GraphQLFieldDefinition.newFieldDefinition()
                .name("clientMutationId").type(GraphQLString)
        for (Map.Entry<String, GraphQLArgument> entry in graphQLDirectiveArgumentMap) clientMutationIdFieldBuilder.argument(entry.getValue())
        clientMutationIdField = clientMutationIdFieldBuilder.build()
        graphQLFieldMap.put("clientMutationId" + KEY_SPLITTER + "String", clientMutationIdField)

        // Predefined GraphQLArgument
        ifArgument = GraphQLArgument.newArgument().name("if")
                .type(GraphQLBoolean).description("Directive @if").build()
        graphQLDirectiveArgumentMap.put("if", ifArgument)

        // Predefined GraphQLObject
        pageInfoType = GraphQLObjectType.newObject().name("GraphQLPageInfo")
                .field(getGraphQLFieldWithNoArgs("pageIndex", GraphQLInt, ""))
                .field(getGraphQLFieldWithNoArgs("pageSize", GraphQLInt, ""))
                .field(getGraphQLFieldWithNoArgs("totalCount", GraphQLInt, ""))
                .field(getGraphQLFieldWithNoArgs("pageMaxIndex", GraphQLInt, ""))
                .field(getGraphQLFieldWithNoArgs("pageRangeLow", GraphQLInt, ""))
                .field(getGraphQLFieldWithNoArgs("pageRangeHigh", GraphQLInt, ""))
                .field(getGraphQLFieldWithNoArgs("hasPreviousPage", GraphQLBoolean, "hasPreviousPage will be false if the client is not paginating with last, or if the client is paginating with last, and the server has determined that the client has reached the end of the set of edges defined by their cursors."))
                .field(getGraphQLFieldWithNoArgs("hasNextPage", GraphQLBoolean, "hasNextPage will be false if the client is not paginating with first, or if the client is paginating with first, and the server has determined that the client has reached the end of the set of edges defined by their cursors"))
                .field(getGraphQLFieldWithNoArgs("startCursor", GraphQLString, ""))
                .field(getGraphQLFieldWithNoArgs("endCursor", GraphQLString, ""))
                .build()
        graphQLObjectTypeMap.put("GraphQLPageInfo", pageInfoType)
        graphQLOutputTypeMap.put("GraphQLPageInfo", pageInfoType)

        // Predefined GraphQLInputObject
        paginationInputType = GraphQLInputObjectType.newInputObject().name("PaginationInputType")
                .field(createPredefinedInputField("pageIndex", GraphQLInt, "0", "Page index for pagination, default 0"))
                .field(createPredefinedInputField("pageSize", GraphQLInt, 20, "Page size for pagination, default 20"))
                .field(createPredefinedInputField("pageNoLimit", GraphQLBoolean, false, "Page no limit for pagination, default false"))
                .field(createPredefinedInputField("orderByField", GraphQLString, null, "OrderBy field for pagination. \ne.g. \n" +
                                     "productName \n" + "productName,statusId \n" + "-statusId,productName"))
                .field(createPredefinedInputField("first", GraphQLInt, 20, "Forward pagination argument takes a non‐negative integer, default 20"))
                .field(createPredefinedInputField("after", GraphQLString, null, "Forward pagination argument takes the cursor, default null"))
                .field(createPredefinedInputField("last", GraphQLInt, 20, "Backward pagination argument takes a non‐negative integer, default 20"))
                .field(createPredefinedInputField("before", GraphQLString, null, "Backward pagination argument takes the cursor, default null"))
                .build()
        graphQLInputTypeMap.put("PaginationInputType", paginationInputType)

        operationInputType = GraphQLInputObjectType.newInputObject().name("OperationInputType")
                .field(createPredefinedInputField("op", GraphQLString, null, "Operation on field, one of [ equals | like | contains | begins | empty | in ]"))
                .field(createPredefinedInputField("value", GraphQLString, null, "Argument value"))
                .field(createPredefinedInputField("not", GraphQLString, null, "Not operation, one of [ Y | true ] represents true"))
                .field(createPredefinedInputField("ic", GraphQLString, null, "Case insensitive, one of [ Y | true ] represents true"))
                .build()
        graphQLInputTypeMap.put("OperationInputType", operationInputType)

        dateRangeInputType = GraphQLInputObjectType.newInputObject().name("DateRangeInputType")
                .field(createPredefinedInputField("period", GraphQLChar, null, ""))
                .field(createPredefinedInputField("poffset", GraphQLChar, null, ""))
                .field(createPredefinedInputField("from", GraphQLChar, null, ""))
                .field(createPredefinedInputField("thru", GraphQLChar, null, ""))
                .build()
        graphQLInputTypeMap.put("DateRangeInputType", dateRangeInputType)

        paginationArgument = GraphQLArgument.newArgument().name("pagination")
                .type(paginationInputType)
                .description("pagination").build()
        graphQLArgumentMap.put(getArgumentKey("pagination", paginationInputType.name), paginationArgument)

        clientMutationIdInputField = GraphQLInputObjectField.newInputObjectField().name("clientMutationId")
                .type(GraphQLString).description("A unique identifier for the client performing the mutation.")
                .build()
        graphQLInputObjectFieldMap.put("clientMutationId", clientMutationIdInputField)
    }

    GraphQLSchemaDefinition(ExecutionContextFactory ecf, Map<String, MNode> schemaNodeMap) {
        this.ecf = ecf

        this.schemaNodeMap = schemaNodeMap.sort { Map.Entry<String, MNode> it ->
            String priority = it.value.attribute('load-priority') ?: "99"
            return priority.toInteger()
        }

        GraphQLSchemaUtil.createObjectTypeNodeForAllEntities(ecf, allTypeDefMap)
        GraphQLSchemaUtil.createObjectTypeNodeForAllDataDocuments(ecf, allTypeDefMap)

        for (Map.Entry<String, MNode> entry in this.schemaNodeMap) {
            MNode schemaNode = entry.value
            for (MNode interfaceFetcherNode in schemaNode.children("interface-fetcher")) {
                interfaceFetcherNodeMap.put(interfaceFetcherNode.attribute("name"), interfaceFetcherNode)
            }
        }

        for (Map.Entry<String, MNode> entry in this.schemaNodeMap) {
            MNode schemaNode = entry.value
            logger.info("Loading graphql schema configuration from ${entry.key}")

            String rootFieldName = schemaNode.attribute("name")
            String rootQueryTypeName = schemaNode.attribute("query")
            String rootMutationTypeName = schemaNode.attribute("mutation")

            if (rootQueryTypeName) queryRootFieldMap.put(rootFieldName, rootQueryTypeName)

            if (rootMutationTypeName) mutationRootFieldMap.put(rootFieldName, rootMutationTypeName)

            for (MNode childNode in schemaNode.children) {
                switch (childNode.name) {
                    case "input-type":
                        schemaInputTypeNameList.add(childNode.attribute("name"))
                        break
                    case "interface":
                        InterfaceTypeDefinition interfaceTypeDef = new InterfaceTypeDefinition(childNode, ecf)
                        allTypeDefMap.put(childNode.attribute("name"), interfaceTypeDef)
                        interfaceTypeDefMap.put(childNode.attribute("name"), interfaceTypeDef)
                        break
                    case "object":
                        allTypeDefMap.put(childNode.attribute("name"), new ObjectTypeDefinition(childNode, ecf))
                        break
                    case "union":
                        allTypeDefMap.put(childNode.attribute("name"), new UnionTypeDefinition(childNode))
                        break
                    case "enum":
                        allTypeDefMap.put(childNode.attribute("name"), new EnumTypeDefinition(childNode))
                        break
                    case "extend-object":
                        extendObjectDefMap.put(childNode.attribute("name"),
                                               mergeExtendObjectDef(extendObjectDefMap, new ExtendObjectDefinition(childNode, ecf)))
                        break
                }
            }
        }

        createRootObjectTypeDef(queryRootObjectTypeName, queryRootFieldMap)
        createRootObjectTypeDef(mutationRootObjectTypeName, mutationRootFieldMap)

        updateAllTypeDefMap()
    }

    private static ExtendObjectDefinition mergeExtendObjectDef(Map<String, ExtendObjectDefinition> extendObjectDefMap, ExtendObjectDefinition extendObjectDef) {
        ExtendObjectDefinition eoDef = extendObjectDefMap.get(extendObjectDef.name)
        if (eoDef == null) return extendObjectDef
        return eoDef.merge(extendObjectDef)
    }

    private void createRootObjectTypeDef(String rootObjectTypeName, Map<String, String> rootFieldMap) {
        Map<String, FieldDefinition> fieldDefMap = new LinkedHashMap<>()
        for (Map.Entry<String, String> entry in rootFieldMap) {
            String fieldName = entry.getKey()
            String fieldTypeName = entry.getValue()
            Map<String, String> fieldPropertyMap =  [nonNull: "true"]

            FieldDefinition fieldDef = getCachedFieldDefinition(fieldName, fieldTypeName, fieldPropertyMap.nonNull, "false", "false")
            if (fieldDef == null) {
                fieldDef = new FieldDefinition(ecf, fieldName, fieldTypeName, fieldPropertyMap)
                fieldDef.setDataFetcher(new EmptyDataFetcher(fieldDef))
                putCachedFieldDefinition(fieldDef)
            }
            fieldDefMap.put(fieldName, fieldDef)
        }

        if (fieldDefMap.size() == 0) {
            FieldDefinition fieldDef = new FieldDefinition(ecf, "empty", "String", [nonNull: "false"])
            fieldDefMap.put("empty", fieldDef)
        }
        ObjectTypeDefinition objectTypeDef = new ObjectTypeDefinition(ecf, rootObjectTypeName, "", new ArrayList<String>(), fieldDefMap)
        allTypeDefMap.put(rootObjectTypeName,  objectTypeDef)
    }

    static FieldDefinition getCachedFieldDefinition(String name, String rawTypeName, String nonNull, String isList, String listItemNonNull) {
        return fieldDefMap.get(getFieldKey(name, rawTypeName, nonNull, isList, listItemNonNull))
    }

    static void putCachedFieldDefinition(FieldDefinition fieldDef) {
        String fieldKey = getFieldKey(fieldDef.name, fieldDef.type, fieldDef.nonNull, fieldDef.isList, fieldDef.listItemNonNull)
        if (fieldDefMap.get(fieldKey) != null)
            throw new IllegalArgumentException("FieldDefinition [${fieldDef.name} - ${fieldDef.type}] already exists in cache")
        fieldDefMap.put(fieldKey, fieldDef)
    }

    static ArgumentDefinition getCachedArgumentDefinition(String name, String type, String required) {
        return argumentDefMap.get(getArgumentKey(name, type, required))
    }

    static String getArgumentTypeName(String type, String fieldIsList) {
        if (!"true".equals(fieldIsList)) return type
        if (GraphQLSchemaUtil.graphQLStringTypes.contains(type) || GraphQLSchemaUtil.graphQLNumericTypes.contains(type) ||
                GraphQLSchemaUtil.graphQLDateTypes.contains(type))
            return operationInputType.name
        if (GraphQLSchemaUtil.graphQLDateTypes.contains(type)) return dateRangeInputType.name

        return type
    }

    static void putCachedArgumentDefinition(ArgumentDefinition argDef) {
        if (!(GraphQLSchemaUtil.graphQLScalarTypes.containsKey(argDef.type) ||
                dateRangeInputType.name.equals(argDef.type) ||
                operationInputType.name.equals(argDef.type))) return

        String argumentKey = getArgumentKey(argDef.name, argDef.type, argDef.required)
        if (argumentDefMap.get(argumentKey) != null)
            throw new IllegalArgumentException("ArgumentDefinition [${argDef.name} - ${argDef.type}] already exists in cache")
        argumentDefMap.put(argumentKey, argDef)
    }

    static String getArgumentKey(String name, String type) {
        return getArgumentKey(name, type, null)
    }

    static String getArgumentKey(String name, String type, String required) {
        String argumentKey = name + KEY_SPLITTER + type
        if ("true".equals(required)) argumentKey = argumentKey + REQUIRED_SUFFIX
        return argumentKey
    }

    static void clearAllCachedGraphQLTypes() {
        graphQLOutputTypeMap.clear()
        graphQLInputTypeMap.clear()
        graphQLEnumTypeMap.clear()
        graphQLUnionTypeMap.clear()
        graphQLInterfaceTypeMap.clear()
        graphQLObjectTypeMap.clear()
        graphQLFieldMap.clear()
        graphQLInputObjectTypeMap.clear()
        graphQLInputObjectFieldMap.clear()
        graphQLArgumentMap.clear()
        graphQLDirectiveArgumentMap.clear()
        graphQLTypeReferenceMap.clear()

        fieldDefMap.clear()
        argumentDefMap.clear()

        interfaceFetcherNodeMap.clear()
        interfaceResolverTypeSet.clear()

        createPredefinedGraphQLTypes()
    }

    void clearAllCachedDefs() {
        queryRootFieldMap.clear()
        mutationRootFieldMap.clear()

        schemaInputTypeMap.clear()
        schemaInputTypeNameList.clear()
        allTypeDefMap.clear()
        allTypeDefSortedList.clear()
        requiredTypeDefMap.clear()
        interfaceTypeDefMap.clear()
        fieldDefMap.clear()
        argumentDefMap.clear()
        extendObjectDefMap.clear()
    }

    private GraphQLTypeDefinition getTypeDef(String name) {
        return allTypeDefMap.get(name)
//        return allTypeNodeList.find({ name.equals(it.name) })
    }

    private void addSchemaInputTypes() {
        // Add default GraphQLScalarType
        for (Map.Entry<String, GraphQLScalarType> entry in GraphQLSchemaUtil.graphQLScalarTypes.entrySet()) {
            schemaInputTypeMap.put(entry.getKey(), entry.getValue())
        }

        schemaInputTypeMap.put(paginationInputType.name, paginationInputType)
        schemaInputTypeMap.put(operationInputType.name, operationInputType)
        schemaInputTypeMap.put(dateRangeInputType.name, dateRangeInputType)

        // Add explicitly defined input types from *.graphql.xml
        for (String inputTypeName in schemaInputTypeNameList) {
            GraphQLInputType type = graphQLInputTypeMap.get(inputTypeName)
            if (type == null)
                throw new IllegalArgumentException("GraphQLInputType [${inputTypeName}] for schema not found")
            schemaInputTypeMap.put(inputTypeName, type)
        }

        addSchemaInputObjectTypes()
    }

    // Create InputObjectType (Input) for mutation fields
    private void addSchemaInputObjectTypes() {
        ExecutionContextFactoryImpl ecfi = (ExecutionContextFactoryImpl) ecf
        for (Map.Entry<String, GraphQLTypeDefinition> entry in allTypeDefMap) {
            if (!(entry.getValue() instanceof ObjectTypeDefinition)) continue
            for (FieldDefinition fieldDef in ((ObjectTypeDefinition) entry.getValue()).fieldList) {
                if (!fieldDef.isMutation) continue
                if (fieldDef.dataFetcher == null)
                    throw new IllegalArgumentException("FieldDefinition [${fieldDef.name} - ${fieldDef.type}] as mutation must have a data fetcher")
                if (fieldDef.dataFetcher instanceof EmptyDataFetcher)
                    throw new IllegalArgumentException("FieldDefinition [${fieldDef.name} - ${fieldDef.type}] as mutation can't have empty data fetcher")

                if (fieldDef.dataFetcher instanceof ServiceDataFetcher) {
                    String serviceName = ((ServiceDataFetcher) fieldDef.dataFetcher).serviceName
                    String inputTypeName = GraphQLSchemaUtil.camelCaseToUpperCamel(fieldDef.name) + "Input"

                    boolean isEntityAutoService = ((ServiceDataFetcher) fieldDef.dataFetcher).isEntityAutoService

                    Map<String, InputObjectFieldDefinition> inputFieldMap
                    if (isEntityAutoService) {
                        // Entity Auto Service only works for mutation which is checked in ServiceDataFetcher initialization.
                        String verb = ServiceDefinition.getVerbFromName(serviceName)
                        String entityName = ServiceDefinition.getNounFromName(serviceName)
                        EntityDefinition ed = ecfi.entityFacade.getEntityDefinition(entityName)

                        ArrayList<String> allFields = verb == 'delete' ? ed.getPkFieldNames() : ed.getAllFieldNames()
                        inputFieldMap = new LinkedHashMap<>(allFields.size())

                        for (int i = 0; i < allFields.size(); i++) {
                            FieldInfo fi = ed.getFieldInfo(allFields.get(i))
                            String inputFieldType = GraphQLSchemaUtil.fieldTypeGraphQLMap.get(fi.type)
                            Object defaultValue = null
                            InputObjectFieldDefinition inputFieldDef = new InputObjectFieldDefinition(fi.name, inputFieldType, defaultValue, "")
                            inputFieldMap.put(fi.name, inputFieldDef)
                        }
                    } else {
                        ServiceDefinition sd = ecfi.serviceFacade.getServiceDefinition(serviceName)

                        // logger.info("======== inputTypeName - ${inputTypeName}")
                        inputFieldMap = new LinkedHashMap<>(sd.getInParameterNames().size())
                        for (String parmName in sd.getInParameterNames()) {
                            MNode parmNode = sd.getInParameter(parmName)
                            Object defaultValue = null

                            String inputFieldNonNull = isServiceParameterRequired(parmNode)
                            String inputFieldIsList = GraphQLSchemaUtil.getShortJavaType(parmNode.attribute("type")) == "List" ? "true" : "false"
                            GraphQLInputType fieldInputType =  getInputTypeRecursiveInSD(parmNode, inputTypeName)

                            InputObjectFieldDefinition inputFieldDef = new InputObjectFieldDefinition(parmName, fieldInputType.name, defaultValue, "",
                                    inputFieldNonNull, inputFieldIsList, "false")
                            inputFieldMap.put(parmName, inputFieldDef)
                        }
                    }
                    GraphQLInputObjectType.Builder inputObjectTypeBuilder = GraphQLInputObjectType.newInputObject()
                            .name(inputTypeName).description("Autogenerated input type of ${inputTypeName}")

                    for (Map.Entry<String, InputObjectFieldDefinition> inputFieldEntry in inputFieldMap) {
                        InputObjectFieldDefinition inputFieldDef = inputFieldEntry.getValue()

                        if ("clientMutationId".equals(inputFieldDef.name)) continue

                        inputObjectTypeBuilder.field(buildSchemaInputField(inputFieldDef))
                    }
                    inputObjectTypeBuilder.field(clientMutationIdInputField)
                    GraphQLInputObjectType inputObjectType = inputObjectTypeBuilder.build()
                    graphQLInputTypeMap.put(inputTypeName, inputObjectType)
                }
            }
        }
    }

    private GraphQLInputType getInputTypeRecursiveInSD(MNode node, String inputTypeNamePrefix) {
        // default to String
        if (node == null) return GraphQLString

        String parmName = node.attribute("name")
        String parmType = node.attribute("type")
        String inputTypeName = GraphQLSchemaUtil.getGraphQLTypeNameByJava(parmType)

        GraphQLScalarType scalarType = GraphQLSchemaUtil.graphQLScalarTypes.get(inputTypeName)
        if (scalarType) return scalarType

        inputTypeName = inputTypeNamePrefix + '_' + parmName

        GraphQLInputType inputType = graphQLInputTypeMap.get(inputTypeName)
        if (inputType) return inputType

        switch (parmType) {
            case "List":
                if (node.childNodes.size() > 1) throw new IllegalArgumentException("Parameter ${parmName} as List can't have more than one children")
                MNode listItemNode = node.children.size() > 0 ? node.child(0) : null

                GraphQLInputType listItemInputType = getInputTypeRecursiveInSD(listItemNode, inputTypeName)

                inputType = listItemInputType
                break
            case "Map":
                if (node.children.size() == 0) throw new IllegalArgumentException("Parameter ${parmName} as Map must has at least one child parameter node")

                GraphQLInputObjectType.Builder builder = GraphQLInputObjectType.newInputObject()
                    .name(inputTypeName)

                for (MNode mapEntryNode in node.children) {
                    GraphQLInputType mapEntryRawType = getInputTypeRecursiveInSD(mapEntryNode, inputTypeName)
                    String mapEntryNonMull = isServiceParameterRequired(mapEntryNode)
                    String mapEntryIsList = GraphQLSchemaUtil.getShortJavaType(mapEntryNode.attribute("type")) == "List" ? "true" : "false"

                    GraphQLInputObjectField inputObjectField = GraphQLInputObjectField.newInputObjectField()
                            .name(mapEntryNode.attribute("name"))
                            .type(getGraphQLInputType(mapEntryRawType, mapEntryNonMull, mapEntryIsList, "false"))
                            .build()
                    builder.field(inputObjectField)
                }
                inputType = builder.build()
                break
            default:
                throw new IllegalArgumentException("Type ${inputTypeName} - ${parmType} for input field is not supported")
                break
        }

        graphQLInputTypeMap.put(inputTypeName, inputType)
        return inputType
    }

    private static String isServiceParameterRequired(MNode parmNode) {
        if (parmNode.attribute("default") || parmNode.attribute("default-value")) return "false"
        return parmNode.attribute("required") ?: "false"
    }

    private void populateSortedTypes() {
        allTypeDefSortedList.clear()

        GraphQLTypeDefinition queryTypeDef = getTypeDef(queryRootObjectTypeName)
        GraphQLTypeDefinition mutationTypeDef = getTypeDef(mutationRootObjectTypeName)

        TreeNode<GraphQLTypeDefinition> rootNode = new TreeNode<>(null)
        TreeNode<GraphQLTypeDefinition> interfaceNode = new TreeNode<>(null)

        for (Map.Entry<String, InterfaceTypeDefinition> entry in interfaceTypeDefMap)
            interfaceNode.children.add(new TreeNode<GraphQLTypeDefinition>((InterfaceTypeDefinition) entry.getValue()))

        TreeNode<GraphQLTypeDefinition> queryTypeNode = new TreeNode<GraphQLTypeDefinition>(queryTypeDef)
        rootNode.children.add(queryTypeNode)

        List<String> objectTypeNames = [queryRootObjectTypeName, mutationRootObjectTypeName]

        createTreeNodeRecursive(interfaceNode, objectTypeNames, true)
        traverseByPostOrder(interfaceNode, allTypeDefSortedList)


        createTreeNodeRecursive(queryTypeNode, objectTypeNames, false)
        traverseByPostOrder(queryTypeNode, allTypeDefSortedList)

        if (mutationTypeDef) {
            TreeNode<GraphQLTypeDefinition> mutationTypeNode = new TreeNode<GraphQLTypeDefinition>(mutationTypeDef)
            rootNode.children.add(mutationTypeNode)
            createTreeNodeRecursive(mutationTypeNode, objectTypeNames, false)
            traverseByPostOrder(mutationTypeNode, allTypeDefSortedList)
        }

        for (Map.Entry<String, GraphQLTypeDefinition> entry in requiredTypeDefMap) {
            if (allTypeDefSortedList.contains(entry.getValue())) continue
            allTypeDefSortedList.add((GraphQLTypeDefinition) entry.getValue())
        }

//        logger.info("==== allTypeNodeSortedList begin ====")
//        for (GraphQLTypeDefinition typeDef in allTypeDefSortedList) {
//            logger.info("[${typeDef.name} - ${typeDef.type}]")
//        }
//        logger.info("==== allTypeNodeSortedList end ====")
    }

    private void traverseByLevelOrder(TreeNode<GraphQLTypeDefinition> startNode, LinkedList<GraphQLTypeDefinition> sortedList) {
        Queue<TreeNode<GraphQLTypeDefinition>> queue = new LinkedList<>()
        queue.add(startNode)
        while(!queue.isEmpty()) {
            TreeNode<GraphQLTypeDefinition> tempNode = queue.poll()
            if (tempNode.data) {
//                logger.info("Traversing node [${tempNode.data.name}]")
                if (!sortedList.contains(tempNode.data)) {
                    sortedList.addFirst(tempNode.data)
                }
            }
            for (TreeNode<GraphQLTypeDefinition> childNode in tempNode.children) {
                queue.add(childNode)
            }
        }
    }

    private void traverseByPostOrder(TreeNode<GraphQLTypeDefinition> startNode, LinkedList<GraphQLTypeDefinition> sortedList) {
        if (startNode == null) return

        for (TreeNode<GraphQLTypeDefinition> childNode in startNode.children) {
            traverseByPostOrder(childNode, sortedList)
        }

        if (startNode.data == null) return
//        logger.info("Post order traversing node [${startNode.data.name}]")
        if (!sortedList.contains(startNode.data)) {
            sortedList.add(startNode.data)
        }
    }

    private void createTreeNodeRecursive(TreeNode<GraphQLTypeDefinition> node, List<String> objectTypeNames, boolean includeInterface) {
        if (node.data) {
            for (String type in node.data.getDependentTypes()) {
                // If type is GraphQL Scalar types, skip.
                if (GraphQLSchemaUtil.graphQLScalarTypes.containsKey(type)) continue
                // If type is GraphQLObjectType which already added in Tree, skip.
                if (objectTypeNames.contains(type)) continue
                if (!includeInterface && "interface".equals(type)) continue

                GraphQLTypeDefinition typeDef = getTypeDef(type)
                if (typeDef != null) {
                    TreeNode<GraphQLTypeDefinition> typeTreeNode = new TreeNode<>(typeDef)
                    node.children.add(typeTreeNode)
                    objectTypeNames.push(type)
//                    logger.info("Adding tree node for GraphQLTypeDefinition [${typeDef.name}]")
                    createTreeNodeRecursive(typeTreeNode, objectTypeNames, includeInterface)
                } else {
                    logger.error("No GraphQL Type [${type}] defined")
                }
            }
        } else {
            for (TreeNode<GraphQLTypeDefinition> childTreeNode in node.children) {
                createTreeNodeRecursive(childTreeNode, objectTypeNames, includeInterface)
            }
        }
    }

    GraphQLSchema getSchema() {
        addSchemaInputTypes()
        populateSortedTypes()

        // Initialize interface type first to prevent null reference when initialize object type
//        for (GraphQLTypeDefinition typeDef in allTypeDefSortedList) {
//            if (!("interface".equals(typeDef.type))) continue
//            addGraphQLInterfaceType((InterfaceTypeDefinition) typeDef)
//        }

        for (GraphQLTypeDefinition typeDef in allTypeDefSortedList) {
            logger.info("Traversing allTypeDefSortedList: ${typeDef.name}")
            switch (typeDef.type) {
                case "union":
                    addGraphQLUnionType((UnionTypeDefinition) typeDef)
                    break
                case "enum":
                    addGraphQLEnumType((EnumTypeDefinition) typeDef)
                    break
                case "interface":
                    addGraphQLInterfaceType((InterfaceTypeDefinition) typeDef)
                    break
                case "object":
                    addGraphQLObjectType((ObjectTypeDefinition) typeDef)
                    break
            }
        }
        rebuildQueryObjectType()

        // Create GraphQLSchema
        GraphQLObjectType schemaQueryType = graphQLObjectTypeMap.get(this.queryRootObjectTypeName)

        GraphQLSchema.Builder schemaBuilder = GraphQLSchema.newSchema().query(schemaQueryType)

        if (mutationRootFieldMap.size() > 0) {
            GraphQLObjectType schemaMutationType = graphQLObjectTypeMap.get(this.mutationRootObjectTypeName)
            schemaBuilder = schemaBuilder.mutation(schemaMutationType)
        }

        GraphQLSchema schema = schemaBuilder.build(new HashSet<GraphQLType>(schemaInputTypeMap.values()))

//        hackCallReplaceTypeReferences(schema)

        logger.info("Schema loaded: " +
                "${graphQLUnionTypeMap.size()} union type, " +
                "${graphQLEnumTypeMap.size()} enum type, " +
                "${graphQLInterfaceTypeMap.size()} interface type, " +
                "${graphQLObjectTypeMap.size()} object type, " +
                "${graphQLFieldMap.size()} fields, " +
                "${graphQLInputTypeMap.size()} input types, " +
                "${graphQLInputObjectFieldMap.size()} input field")

        // clear all caches defs
        clearAllCachedDefs()
        
        return schema
    }

    private static void hackCallReplaceTypeReferences(GraphQLSchema schema) {
//        Class<?> schemaUtilClass = Thread.getClassLoader().loadClass("graphql.schema.SchemaUtil")
        SchemaUtil util = new SchemaUtil()

//        Method method = schemaUtilClass.getMethod("replaceTypeReferences", GraphQLSchema.class)
        Method method = util.getClass().getDeclaredMethod("replaceTypeReferences", GraphQLSchema.class)
        method.setAccessible(true)
        method.invoke(util, schema)
    }

    private void updateAllTypeDefMap() {
        // Extend object which convert to interface first
        for (Map.Entry<String, ExtendObjectDefinition> entry in extendObjectDefMap) {
            ExtendObjectDefinition extendObjectDef = (ExtendObjectDefinition) entry.getValue()
            if (!extendObjectDef.convertToInterface) continue

            String name = entry.getKey()
            ObjectTypeDefinition objectTypeDef = (ObjectTypeDefinition) allTypeDefMap.get(name)
            if (objectTypeDef == null)
                throw new IllegalArgumentException("ObjectTypeDefinition [${name}] not found to extend")

            if (interfaceTypeDefMap.containsKey(name))
                throw new IllegalArgumentException("Interface [${name}] to be extended already exists")

            InterfaceTypeDefinition interfaceTypeDef = new InterfaceTypeDefinition(objectTypeDef, extendObjectDef, ecf)
            allTypeDefMap.put(interfaceTypeDef.name, interfaceTypeDef)
            interfaceTypeDefMap.put(interfaceTypeDef.name, interfaceTypeDef)

            objectTypeDef.convertToInterface = true
            objectTypeDef.extend(extendObjectDef, allTypeDefMap)
            // Interface need the object to do resolve
            requiredTypeDefMap.put(objectTypeDef.name, objectTypeDef)
        }

        // Extend object
        for (Map.Entry<String, ExtendObjectDefinition> entry in extendObjectDefMap) {
            ExtendObjectDefinition extendObjectDef = (ExtendObjectDefinition) entry.getValue()

            String name = entry.getKey()
            ObjectTypeDefinition objectTypeDef = (ObjectTypeDefinition) allTypeDefMap.get(name)
            if (objectTypeDef == null)
                throw new IllegalArgumentException("ObjectTypeDefinition [${name}] not found to extend")

            objectTypeDef.extend(extendObjectDef, allTypeDefMap)
        }

    }

    private static GraphQLOutputType getConnectionObjectType(String rawTypeName, String nonNull, String listItemNonNull) {
        GraphQLOutputType rawType = graphQLOutputTypeMap.get(rawTypeName)
        if (rawType == null) {
            rawType = graphQLTypeReferenceMap.get(rawTypeName)
            if (rawType == null) {
                rawType = new GraphQLTypeReference(rawTypeName)
                logger.info("Adding graphQLTypeReferenceMap: ${rawTypeName}")
                graphQLTypeReferenceMap.put(rawTypeName, (GraphQLTypeReference) rawType)

            }
        }
        return getConnectionObjectType(rawType, nonNull, listItemNonNull)
    }

    private static GraphQLOutputType getConnectionObjectType(GraphQLOutputType rawType, String nonNull, String listItemNonNull) {
        String connectionTypeName = rawType.name + "Connection"
        String connectionTypeKey = rawType.name
        if ("true".equals(nonNull)) connectionTypeKey = connectionTypeKey + NON_NULL_SUFFIX
        connectionTypeKey = connectionTypeKey + IS_LIST_SUFFIX
        if ("true".equals(listItemNonNull)) connectionTypeKey = connectionTypeKey + LIST_ITEM_NON_NULL_SUFFIX

        GraphQLOutputType wrappedConnectionType = graphQLOutputTypeMap.get(connectionTypeKey)
        if (wrappedConnectionType != null) return wrappedConnectionType

        GraphQLOutputType connectionType = graphQLOutputTypeMap.get(connectionTypeName)
        if (connectionType == null) {
            connectionType = GraphQLObjectType.newObject().name(connectionTypeName)
                    .field(getEdgesField(rawType, nonNull, listItemNonNull))
                    .field(getGraphQLFieldWithNoArgs("pageInfo", pageInfoType, "true", "false", "false", null))
                    .build()
            graphQLOutputTypeMap.put(connectionTypeName, connectionType)
        }

        wrappedConnectionType = connectionType
        if ("true".equals(nonNull)) wrappedConnectionType = new GraphQLNonNull(connectionType)

        if (!connectionTypeKey.equals(connectionTypeName)) graphQLOutputTypeMap.put(connectionTypeKey, wrappedConnectionType)

        return wrappedConnectionType
    }

    private static GraphQLFieldDefinition getEdgesField(GraphQLOutputType rawType, String nonNull, String listItemNonNull) {
        String edgesFieldName = "edges"
        String edgeFieldKey = edgesFieldName + KEY_SPLITTER + rawType.name + "Edge"
        if ("true".equals(nonNull)) edgeFieldKey = edgeFieldKey + NON_NULL_SUFFIX
        edgeFieldKey = edgeFieldKey + IS_LIST_SUFFIX
        if ("true".equals(listItemNonNull)) edgeFieldKey = edgeFieldKey + LIST_ITEM_NON_NULL_SUFFIX

        GraphQLFieldDefinition edgesField = graphQLFieldMap.get(edgeFieldKey)
        if (edgesField != null) return edgesField

        GraphQLFieldDefinition.Builder edgesFieldBuilder = GraphQLFieldDefinition.newFieldDefinition().name(edgesFieldName)
                .type(getEdgesObjectType(rawType, nonNull, listItemNonNull))

        for (Map.Entry<String, GraphQLArgument> entry in graphQLDirectiveArgumentMap) edgesFieldBuilder.argument(entry.getValue())

        edgesField = edgesFieldBuilder.build()
        graphQLFieldMap.put(edgeFieldKey, edgesField)

        return edgesField
    }

    // Should not be invoked, but getEdgesField
    private static GraphQLOutputType getEdgesObjectType(GraphQLOutputType rawType, String nonNull, String listItemNonNull) {
        String edgeRawTypeName = rawType.name + "Edge"
        String edgesTypeKey = edgeRawTypeName
        if ("true".equals(nonNull)) edgesTypeKey = edgesTypeKey + NON_NULL_SUFFIX
        edgesTypeKey = edgesTypeKey + IS_LIST_SUFFIX
        if ("true".equals(listItemNonNull)) edgesTypeKey = edgesTypeKey + LIST_ITEM_NON_NULL_SUFFIX

        GraphQLOutputType edgesType = graphQLOutputTypeMap.get(edgesTypeKey)
        if (edgesType != null) return edgesType

        GraphQLObjectType edgeRawType = graphQLObjectTypeMap.get(edgeRawTypeName)
        if (edgeRawType == null) {
            GraphQLFieldDefinition nodeField = getGraphQLFieldWithNoArgs("node", rawType, nonNull, "false", listItemNonNull, null)

            edgeRawType = GraphQLObjectType.newObject().name(edgeRawTypeName)
                    .field(cursorField)
                    .field(nodeField).build()
            graphQLObjectTypeMap.put(edgeRawTypeName, edgeRawType)
            graphQLOutputTypeMap.put(edgeRawTypeName, edgeRawType)
        }

        edgesType = edgeRawType

        if ("true".equals(listItemNonNull)) edgesType = new GraphQLNonNull(edgesType)
        edgesType = new GraphQLList(edgesType)
        if ("true".equals(nonNull)) edgesType = new GraphQLNonNull(edgesType)

        if (!edgesTypeKey.equals(edgeRawTypeName)) {
            graphQLOutputTypeMap.put(edgesTypeKey, edgesType)
        }

        return edgesType
    }

    private static GraphQLOutputType getGraphQLOutputType(FieldDefinition fieldDef) {
        return getGraphQLOutputType(fieldDef.type, fieldDef.nonNull, fieldDef.isList, fieldDef.listItemNonNull)
    }

    private static GraphQLOutputType getGraphQLOutputType(String rawTypeName, String nonNull, String isList, String listItemNonNull) {
        GraphQLOutputType rawType = graphQLOutputTypeMap.get(rawTypeName)
        if (rawType == null) {
//            throw new IllegalArgumentException("GraphQLOutputType [${rawTypeName}] not found")
            rawType = graphQLTypeReferenceMap.get(rawTypeName)
            if (rawType == null) {
                rawType = new GraphQLTypeReference(rawTypeName)
                logger.info("Adding graphQLTypeReferenceMap: ${rawTypeName}")
                graphQLTypeReferenceMap.put(rawTypeName, (GraphQLTypeReference) rawType)
            }
        }
        return getGraphQLOutputType(rawType, nonNull, isList, listItemNonNull)
    }

    private static GraphQLOutputType getGraphQLOutputType(GraphQLOutputType rawType, String nonNull, String isList, String listItemNonNull) {
        String outputTypeKey = rawType.name
        if ("true".equals(nonNull)) outputTypeKey = outputTypeKey + NON_NULL_SUFFIX
        if ("true".equals(isList)) {
            outputTypeKey = outputTypeKey + IS_LIST_SUFFIX
            if ("true".equals(listItemNonNull)) outputTypeKey = outputTypeKey + LIST_ITEM_NON_NULL_SUFFIX
        }

        GraphQLOutputType wrappedType = graphQLOutputTypeMap.get(outputTypeKey)
        if (wrappedType != null) return wrappedType

        wrappedType = rawType
        if ("true".equals(isList)) {
            if ("true".equals(listItemNonNull)) wrappedType = new GraphQLNonNull(wrappedType)
            wrappedType = new GraphQLList(wrappedType)
        }
        if ("true".equals(nonNull)) wrappedType = new GraphQLNonNull(wrappedType)

        if (!outputTypeKey.equals(rawType.name)) graphQLOutputTypeMap.put(outputTypeKey, wrappedType)

        return wrappedType
    }

    private static GraphQLFieldDefinition getGraphQLFieldWithNoArgs(FieldDefinition fieldDef) {
        if (fieldDef.argumentList.size() > 0)
            throw new IllegalArgumentException("FieldDefinition [${fieldDef.name}] with type [${fieldDef.type}] has arguments, which should not be cached")
        return getGraphQLFieldWithNoArgs(fieldDef.name, fieldDef.type, fieldDef.nonNull, fieldDef.isList,
                                         fieldDef.listItemNonNull, fieldDef.description, fieldDef.dataFetcher)
    }

    private static GraphQLFieldDefinition getGraphQLFieldWithNoArgs(String name, GraphQLOutputType rawType, String description) {
        return getGraphQLFieldWithNoArgs(name, rawType, "false", "false", "false", description, null)
    }

    private static GraphQLFieldDefinition getGraphQLFieldWithNoArgs(String name, String rawTypeName, String nonNull, String isList,
                                                                    String listItemNonNull, String description, BaseDataFetcher dataFetcher) {
        GraphQLOutputType rawType = graphQLOutputTypeMap.get(rawTypeName)
//        if (rawType == null) throw new IllegalArgumentException("GraphQLOutputType [${rawTypeName}] for field [${name}] not found")
        if (rawType == null) {
            rawType = graphQLTypeReferenceMap.get(rawTypeName)
            if (rawType == null) {
                rawType = new GraphQLTypeReference(rawTypeName)
                logger.info("Adding graphQLTypeReferenceMap: ${rawTypeName}")
                graphQLTypeReferenceMap.put(rawTypeName, (GraphQLTypeReference) rawType)
            }
        }
        return getGraphQLFieldWithNoArgs(name, rawType, nonNull, isList, listItemNonNull, description, dataFetcher)
    }

    private static GraphQLFieldDefinition getGraphQLFieldWithNoArgs(String name, GraphQLOutputType rawType, String nonNull, String isList,
                    String listItemNonNull, BaseDataFetcher dataFetcher) {
        return getGraphQLFieldWithNoArgs(name, rawType, nonNull, isList, listItemNonNull, "", dataFetcher)
    }

    private static GraphQLFieldDefinition getGraphQLFieldWithNoArgs(String name, GraphQLOutputType rawType, String nonNull, String isList,
                                                                    String listItemNonNull, String description, BaseDataFetcher dataFetcher) {
        String fieldKey = getFieldKey(name, rawType.name, nonNull, isList, listItemNonNull)

        GraphQLFieldDefinition field = graphQLFieldMap.get(fieldKey)
        if (field != null) return field


        GraphQLOutputType fieldType

        if ("true".equals(isList)) fieldType = getConnectionObjectType(rawType, nonNull, listItemNonNull)
        else fieldType = getGraphQLOutputType(rawType, nonNull, "false", listItemNonNull)

        GraphQLFieldDefinition.Builder fieldBuilder = GraphQLFieldDefinition.newFieldDefinition()
                .name(name).description(description)
        for (Map.Entry<String, GraphQLArgument> entry in graphQLDirectiveArgumentMap) fieldBuilder.argument(entry.getValue())

        fieldBuilder.type(fieldType)

        if ("true".equals(isList)) fieldBuilder.argument(paginationArgument)
        for (Map.Entry<String, GraphQLArgument> entry in graphQLDirectiveArgumentMap) fieldBuilder.argument((GraphQLArgument) entry.getValue())

        if (dataFetcher != null) {
            fieldBuilder.dataFetcher(dataFetcher)
        }

        field = fieldBuilder.build()
        graphQLFieldMap.put(fieldKey, field)

        return field
    }

    protected static String getFieldKey(String name, String rawTypeName, String nonNull, String isList, String listItemNonNull) {
        String fieldKey = name + KEY_SPLITTER + rawTypeName
        if ("true".equals(nonNull)) fieldKey = fieldKey + NON_NULL_SUFFIX
        if ("true".equals(isList)) {
            fieldKey = fieldKey + IS_LIST_SUFFIX
            if ("true".equals(listItemNonNull)) fieldKey = fieldKey + LIST_ITEM_NON_NULL_SUFFIX
        }
        return fieldKey
    }

    private static int unknownInputDefaultValueNum = 0
    private static String getInputFieldKey(InputObjectFieldDefinition inputFieldDef) {
        return getInputFieldKey(inputFieldDef.name, inputFieldDef.type, inputFieldDef.defaultValue,
                inputFieldDef.nonNull, inputFieldDef.isList, inputFieldDef.listItemNonNull)
    }
    private static String getInputFieldKey(String name, String type, Object defaultValue) {
        getInputFieldKey(name, type, defaultValue, "false", "false", "false")
    }
    private static String getInputFieldKey(String name, String type, Object defaultValue, String nonNull, String isList, String listItemNonNull) {
        String defaultValueKey
        if (defaultValue == null) {
            defaultValueKey = "NULL"
        } else {
            // TODO: generate a unique key based on defaultValue
            defaultValueKey = "UNKNOWN" + Integer.toString(unknownInputDefaultValueNum)
            unknownInputDefaultValueNum++
        }

        String inputFieldKey = name + KEY_SPLITTER + type + KEY_SPLITTER + defaultValueKey
        if ("true".equals(nonNull)) inputFieldKey = inputFieldKey + NON_NULL_SUFFIX
        if ("true".equals(isList)) {
            inputFieldKey = inputFieldKey + IS_LIST_SUFFIX
            if ("true".equals(listItemNonNull)) inputFieldKey = inputFieldKey + LIST_ITEM_NON_NULL_SUFFIX
        }

        return inputFieldKey
    }

    private static GraphQLInputType getGraphQLInputType(InputObjectFieldDefinition inputFieldDef) {
        return getGraphQLInputType(inputFieldDef.type, inputFieldDef.nonNull, inputFieldDef.isList, inputFieldDef.listItemNonNull)
    }

    private static GraphQLInputType getGraphQLInputType(String rawTypeName, String nonNull, String isList, String listItemNonNull) {
        GraphQLInputType rawType = graphQLInputTypeMap.get(rawTypeName)
        if (rawType == null) {
            rawType = graphQLTypeReferenceMap.get(rawTypeName)
            if (rawType == null) {
                rawType = new GraphQLTypeReference(rawTypeName)
                logger.info("Adding graphQLTypeReferenceMap for input type: ${rawTypeName}")
                graphQLTypeReferenceMap.put(rawTypeName, (GraphQLTypeReference) rawType)
            }
        }
        return getGraphQLInputType(rawType, nonNull, isList, listItemNonNull)
    }
    private static GraphQLInputType getGraphQLInputType(GraphQLInputType rawType, String nonNull, String isList, String listItemNonNull) {
        String inputTypeKey = rawType.name
        if ("true".equals(nonNull)) inputTypeKey = inputTypeKey + NON_NULL_SUFFIX
        if ("true".equals(isList)) {
            inputTypeKey = inputTypeKey + IS_LIST_SUFFIX
            if ("true".equals(listItemNonNull)) inputTypeKey = inputTypeKey + LIST_ITEM_NON_NULL_SUFFIX
        }

        GraphQLInputType wrappedType = graphQLInputTypeMap.get(inputTypeKey)
        if (wrappedType != null) return wrappedType

        wrappedType = rawType
        if ("true".equals(isList)) {
            if ("true".equals(listItemNonNull)) wrappedType = new GraphQLNonNull(wrappedType)
            wrappedType = new GraphQLList(wrappedType)
        }
        if ("true".equals(nonNull)) wrappedType = new GraphQLNonNull(wrappedType)

        if (!inputTypeKey.equals(rawType.name)) graphQLInputTypeMap.put(inputTypeKey, wrappedType)

        return wrappedType
    }

    private static GraphQLInputObjectField createPredefinedInputField(String name, GraphQLInputType type,
                                                              Object defaultValue, String description) {
        GraphQLInputObjectField.Builder fieldBuilder = GraphQLInputObjectField.newInputObjectField()
            .name(name).type(type).defaultValue(defaultValue).description(description)

        return fieldBuilder.build()
    }

    private static void addGraphQLUnionType(UnionTypeDefinition unionTypeDef) {
        String unionTypeName = unionTypeDef.name
        GraphQLUnionType unionType = graphQLUnionTypeMap.get(unionTypeName)
        if (unionType != null) return

        GraphQLUnionType.Builder unionTypeBuilder = GraphQLUnionType.newUnionType()
                .name(unionTypeName)
                .description(unionTypeDef.description)

        for (String typeName in unionTypeDef.typeList) {
            GraphQLObjectType possibleType = graphQLObjectTypeMap.get(typeName)
            if (possibleType == null)
                throw new IllegalArgumentException("GraphQLObjectType [${typeName}] as possibleType of GraphQLUnionType [${unionTypeName}] not found")

            unionTypeBuilder.possibleType(possibleType)
        }

        // TODO: Add typeResolver for type, one way is to add a service as resolver

        unionType = unionTypeBuilder.build()
        graphQLUnionTypeMap.put(unionTypeName, unionType)
        graphQLOutputTypeMap.put(unionTypeName, unionType)
    }

    private static void addGraphQLEnumType(EnumTypeDefinition enumTypeDef) {
        String enumTypeName = enumTypeDef.name
        GraphQLEnumType enumType = graphQLEnumTypeMap.get(enumTypeName)
        if (enumType != null) return

        GraphQLEnumType.Builder enumTypeBuilder = GraphQLEnumType.newEnum().name(enumTypeName)
                .description(enumTypeDef.description)

        for (EnumValue valueNode in enumTypeDef.valueList)
            enumTypeBuilder = enumTypeBuilder.value(valueNode.name, valueNode.value, valueNode.description, valueNode.depreciationReason)

        enumType = enumTypeBuilder.build()
        graphQLEnumTypeMap.put(enumTypeName, enumType)
        graphQLInputTypeMap.put(enumTypeName, enumType)
        graphQLOutputTypeMap.put(enumTypeName, enumType)
    }

    private static void addGraphQLInterfaceType(InterfaceTypeDefinition interfaceTypeDef) {
        String interfaceTypeName = interfaceTypeDef.name
        GraphQLInterfaceType interfaceType = graphQLInterfaceTypeMap.get(interfaceTypeName)
        if (interfaceType != null) return

        interfaceResolverTypeSet.addAll(interfaceTypeDef.resolverMap.values())

        GraphQLInterfaceType.Builder interfaceTypeBuilder = GraphQLInterfaceType.newInterface()
                .name(interfaceTypeName)
                .description(interfaceTypeDef.description)

        for (FieldDefinition fieldNode in interfaceTypeDef.fieldList) {
            interfaceTypeBuilder.field(buildSchemaField(fieldNode))
        }

        // TODO: Add typeResolver for type, one way is to add a service as resolver
        if (!interfaceTypeDef.convertFromObjectTypeName.isEmpty()) {
            if (interfaceTypeDef.resolverField == null || interfaceTypeDef.resolverField.isEmpty())
                throw new IllegalArgumentException("Interface definition of ${interfaceTypeName} resolverField not set")

            interfaceTypeBuilder.typeResolver(new TypeResolver() {
                @Override
                GraphQLObjectType getType(Object object) {
                    String resolverFieldValue = ((Map) object).get(interfaceTypeDef.resolverField)
                    String resolvedTypeName = interfaceTypeDef.resolverMap.get(resolverFieldValue)

                    GraphQLObjectType resolvedType = graphQLObjectTypeMap.get(resolvedTypeName)
                    if (resolvedType == null) resolvedType = graphQLObjectTypeMap.get(interfaceTypeDef.defaultResolvedTypeName)
                    return resolvedType
                }
            })
        }

        interfaceType = interfaceTypeBuilder.build()
        graphQLInterfaceTypeMap.put(interfaceTypeName, interfaceType)
        graphQLOutputTypeMap.put(interfaceTypeName, interfaceType)
    }

    private static void addGraphQLObjectType(ObjectTypeDefinition objectTypeDef) {
        String objectTypeName = objectTypeDef.name
        GraphQLObjectType objectType = graphQLObjectTypeMap.get(objectTypeName)
        if (objectType != null) return

        GraphQLObjectType.Builder objectTypeBuilder = GraphQLObjectType.newObject()
                .name(objectTypeName)
                .description(objectTypeDef.description)

        for (String interfaceName in objectTypeDef.interfaceList) {
            GraphQLInterfaceType interfaceType = graphQLInterfaceTypeMap.get(interfaceName)
            if (interfaceType == null)
                throw new IllegalArgumentException("GraphQLInterfaceType [${interfaceName}] for GraphQLObjectType [${objectTypeName}] not found.")

            objectTypeBuilder = objectTypeBuilder.withInterface(interfaceType)
        }

        for (FieldDefinition fieldDef in objectTypeDef.fieldList)
            objectTypeBuilder = objectTypeBuilder.field(buildSchemaField(fieldDef))

        objectType = objectTypeBuilder.build()
        graphQLObjectTypeMap.put(objectTypeName, objectType)
        graphQLOutputTypeMap.put(objectTypeName, objectType)
    }

    private void rebuildQueryObjectType() {
        ObjectTypeDefinition queryObjectTypeDef = (ObjectTypeDefinition) allTypeDefMap.get(queryRootObjectTypeName)

        GraphQLObjectType.Builder queryObjectTypeBuilder = GraphQLObjectType.newObject()
                .name(queryRootObjectTypeName)
                .description(queryObjectTypeDef.description)

        for (FieldDefinition fieldDef in queryObjectTypeDef.fieldList)
            queryObjectTypeBuilder = queryObjectTypeBuilder.field(buildSchemaField(fieldDef))

        // create a fake object type
        GraphQLObjectType.Builder graphQLObjectTypeBuilder = GraphQLObjectType.newObject()
                .name("FakeTypeReferenceContainer")
                .description("This is only for contain GraphQLTypeReference so GraphQLSchema includes all of GraphQLTypeReference.")

        boolean hasFakeField = false
        List<String> fakeFieldNameList = new ArrayList<>()
        // fields for GraphQLTypeReference
        for (Map.Entry<String, GraphQLTypeReference> entry in graphQLTypeReferenceMap) {
            if (fakeFieldNameList.contains(entry.key)) continue

            FieldDefinition fieldDef = new FieldDefinition(ecf, entry.key, entry.key)
            graphQLObjectTypeBuilder.field(buildSchemaField(fieldDef))
            fakeFieldNameList.add(entry.key)
            hasFakeField = true
        }

        // fields for resolver type of interface
        for (String resolverType in interfaceResolverTypeSet) {
            if (fakeFieldNameList.contains(resolverType)) continue

            GraphQLTypeDefinition typeDef = getTypeDef(resolverType)
            if (typeDef == null) throw new IllegalArgumentException("GraphQLTypeDefinition ${resolverType} not found")
            addGraphQLObjectType(typeDef as ObjectTypeDefinition)

            FieldDefinition fieldDef = new FieldDefinition(ecf, resolverType, resolverType)
            graphQLObjectTypeBuilder.field(buildSchemaField(fieldDef))
            fakeFieldNameList.add(resolverType)
            hasFakeField = true
        }

        if (hasFakeField) {
            GraphQLObjectType fakeObjectType = graphQLObjectTypeBuilder.build()

            GraphQLFieldDefinition fakeField = GraphQLFieldDefinition.newFieldDefinition()
                    .name("fakeTypeReferenceContainer")
                    .type(fakeObjectType)
                    .build()

            queryObjectTypeBuilder.field(fakeField)
        }

        GraphQLObjectType queryObjectType = queryObjectTypeBuilder.build()

        graphQLObjectTypeMap.put(queryRootObjectTypeName, queryObjectType)
        graphQLOutputTypeMap.put(queryRootObjectTypeName, queryObjectType)
    }

    private static GraphQLFieldDefinition buildSchemaField(FieldDefinition fieldDef) {
        GraphQLFieldDefinition graphQLFieldDef

        if (fieldDef.argumentList.size() == 0 && GraphQLSchemaUtil.graphQLScalarTypes.containsKey(fieldDef.type))
            return getGraphQLFieldWithNoArgs(fieldDef)

        GraphQLOutputType fieldType
        if ("true".equals(fieldDef.isList)) fieldType = getConnectionObjectType(fieldDef.type, fieldDef.nonNull, fieldDef.listItemNonNull)
        else fieldType = getGraphQLOutputType(fieldDef)

        GraphQLFieldDefinition.Builder graphQLFieldDefBuilder = GraphQLFieldDefinition.newFieldDefinition()
                .name(fieldDef.name)
                .type(fieldType)
                .description(fieldDef.description)
                .deprecate(fieldDef.depreciationReason)

        // build arguments for field
        for (ArgumentDefinition argNode in fieldDef.argumentList)
            graphQLFieldDefBuilder.argument(buildSchemaArgument(argNode))
        // Add pagination argument
        if ("true".equals(fieldDef.isList)) graphQLFieldDefBuilder.argument(paginationArgument)
        // Add directive arguments
        for (Map.Entry<String, GraphQLArgument> entry in graphQLDirectiveArgumentMap)
            graphQLFieldDefBuilder.argument((GraphQLArgument) entry.getValue())

        if (fieldDef.dataFetcher != null) {
            graphQLFieldDefBuilder.dataFetcher(fieldDef.dataFetcher)
        }

        graphQLFieldDef = graphQLFieldDefBuilder.build()

        return graphQLFieldDef
    }

    private static GraphQLArgument buildSchemaArgument(ArgumentDefinition argumentDef) {
        String argumentName = argumentDef.name
        GraphQLArgument.Builder argument = GraphQLArgument.newArgument()
                .name(argumentName)
                .description(argumentDef.description)
                .defaultValue(argumentDef.defaultValue)

        GraphQLInputType argType = graphQLInputTypeMap.get(argumentDef.type)
        if (argType == null)
            throw new IllegalArgumentException("GraphQLInputType [${argumentDef.type}] for argument [${argumentName}] not found")

//        if ("true".equals(argumentDef.isOpOrRange)) {
//            if (graphQLDateTypes.contains(argumentDef.type)) {
//                argType = graphQLInputTypeMap.get("DateRangeInputType")
//            } else if (graphQLStringTypes.contains(argumentDef.type) || graphQLNumericTypes.contains(argumentDef.type)) {
//                argType = graphQLInputTypeMap.get("OperationInputType")
//            }
//        }

        argument = argument.type(argType)

        return argument.build()
    }

    private static GraphQLInputObjectField buildSchemaInputField(InputObjectFieldDefinition inputFieldDef) {
        String inputFieldKey = getInputFieldKey(inputFieldDef)
        GraphQLInputObjectField inputObjectField = graphQLInputObjectFieldMap.get(inputFieldKey)
        if (inputObjectField) return inputObjectField

        GraphQLInputType rawType = graphQLInputTypeMap.get(inputFieldDef.type)

        GraphQLInputType wrapperType = rawType
        if ("true".equals(inputFieldDef.isList)) {
            if ("true".equals(inputFieldDef.listItemNonNull)) wrapperType = new GraphQLNonNull(wrapperType)
            wrapperType = new GraphQLList(wrapperType)
        }
        if ("true".equals(inputFieldDef.nonNull)) wrapperType = new GraphQLNonNull(wrapperType)

        GraphQLInputObjectField inputField = GraphQLInputObjectField.newInputObjectField()
                .name(inputFieldDef.name)
                .type(wrapperType)
                .defaultValue(inputFieldDef.defaultValue)
                .description(inputFieldDef.description)
                .build()

        graphQLInputObjectFieldMap.put(inputFieldKey, inputField)
        return inputField
    }

    static class TreeNode<T> {
        T data
        public final List<TreeNode<T>> children = new LinkedList<TreeNode<T>>()

        public TreeNode(data) { this.data = data }
    }

    static abstract class GraphQLTypeDefinition {
        String name, description, type

        List<String> getDependentTypes() { }
    }

    static class EnumValue {
        String name, value, description, depreciationReason

        EnumValue(MNode node) {
            this.name = node.attribute("node")
            this.value = node.attribute("value")
            for (MNode childNode in node.children) {
                switch (childNode.name) {
                    case "description":
                        this.description = childNode.text
                        break
                    case "depreciation-reason":
                        this.depreciationReason = childNode.text
                        break
                }
            }
        }
    }

    static class EnumTypeDefinition extends GraphQLTypeDefinition {
        List<EnumValue> valueList = new LinkedList<>()

        EnumTypeDefinition(MNode node) {
            this.name = node.attribute("name")
            this.type = "enum"

            for (MNode childNode in node.children) {
                switch (childNode.name) {
                    case "description":
                        this.description = childNode.text
                        break
                    case "enum-value":
                        valueList.add(new EnumValue(childNode))
                        break
                }
            }
        }

        @Override
        List<String> getDependentTypes() { return new LinkedList<String>() }
    }

    static  class UnionTypeDefinition extends GraphQLTypeDefinition {
        String typeResolver
        List<String> typeList = new LinkedList<>()

        UnionTypeDefinition(MNode node) {
            this.name = node.attribute("name")
            this.type = "union"
            this.typeResolver = node.attribute("type-resolver")

            for (MNode childNode in node.children) {
                switch (childNode.name) {
                    case "description":
                        this.description = childNode.text
                        break
                    case "type":
                        typeList.add(childNode.attribute("name"))
                        break
                }
            }
        }

        @Override
        List<String> getDependentTypes() { return typeList }
    }


    static class InterfaceTypeDefinition extends GraphQLTypeDefinition {
        @SuppressWarnings("GrFinalVariableAccess")
        final ExecutionContextFactory ecf

        String convertFromObjectTypeName

        String typeResolver
        Map<String, FieldDefinition> fieldDefMap = new LinkedHashMap<>()
        String resolverField
        Map<String, String> resolverMap = new LinkedHashMap<>()
        String defaultResolvedTypeName

        InterfaceTypeDefinition(MNode node, ExecutionContextFactory ecf) {
            this.ecf = ecf
            this.name = node.attribute("name")
            this.type = "interface"
            this.typeResolver = node.attribute("type-resolver")
            for (MNode childNode in node.children) {
                switch (childNode.name) {
                    case "description":
                        this.description = childNode.text
                        break
                    case "field":
                        fieldDefMap.put(childNode.attribute("name"), new FieldDefinition(childNode, ecf))
//                        typeList.add(childNode.attribute("type"))
                        break
                }
            }
        }

        InterfaceTypeDefinition(ObjectTypeDefinition objectTypeDef, ExtendObjectDefinition extendObjectDef, ExecutionContextFactory ecf) {
            this.convertFromObjectTypeName = objectTypeDef.name
            this.ecf = ecf
            this.name = objectTypeDef.name + "Interface"
            this.type = "interface"
            this.defaultResolvedTypeName = objectTypeDef.name
            this.resolverField = extendObjectDef.resolverField
            this.resolverMap.putAll(extendObjectDef.resolverMap)

            fieldDefMap.putAll(objectTypeDef.fieldDefMap)

            for (MNode extendObjectNode in extendObjectDef.extendObjectNodeList) {
                for (MNode fieldNode in extendObjectNode.children("field")) {
                    GraphQLSchemaUtil.mergeFieldDefinition(fieldNode, fieldDefMap, ecf)
                }
            }

            for (String excludeFieldName in extendObjectDef.excludeFields)
                fieldDefMap.remove(excludeFieldName)

            // Make object type that interface convert from extends interface automatically.
            objectTypeDef.interfaceList.add(name)
        }

        public void addResolver(String resolverValue, String resolverType) {
            resolverMap.put(resolverValue, resolverType)
        }

        public List<FieldDefinition> getFieldList() {
            List< FieldDefinition> fieldList = new LinkedList<>()
            for (Map.Entry<String, FieldDefinition> entry in fieldDefMap)
                fieldList.add((FieldDefinition) entry.getValue())
            return fieldList
        }

        @Override
        List<String> getDependentTypes() {
            List<String> typeList = new LinkedList<>()
//            if (!convertFromObjectTypeName.isEmpty()) typeList.add(convertFromObjectTypeName)
            for (Map.Entry<String, FieldDefinition> entry in fieldDefMap)
                typeList.add(((FieldDefinition) entry.getValue()).type)
            return typeList
        }
    }

    static class ObjectTypeDefinition extends GraphQLTypeDefinition {
        @SuppressWarnings("GrFinalVariableAccess")
        final ExecutionContextFactory ecf

        String convertToInterface
        List<String> interfaceList = new LinkedList<>()
        Map<String, InterfaceTypeDefinition> interfacesMap
        Map<String, FieldDefinition> fieldDefMap = new LinkedHashMap<>()

        ObjectTypeDefinition(MNode node, ExecutionContextFactory ecf) {
            this.ecf = ecf
            this.name = node.attribute("name")
            this.type = "object"
            for (MNode childNode in node.children) {
                switch (childNode.name) {
                    case "description":
                        this.description = childNode.text
                        break
                    case "interface":
                        interfaceList.add(childNode.attribute("name"))
                        break
                    case "field":
                        fieldDefMap.put(childNode.attribute("name"), new FieldDefinition(childNode, ecf))
//                        typeList.add(childNode.attribute("type"))
                        break
                }
            }
        }

        ObjectTypeDefinition(ExecutionContextFactory ecf, String name, String description, List<String> interfaceList,
                             Map<String, FieldDefinition> fieldDefMap) {
            this.ecf = ecf
            this.name = name
            this.description = description
            this.type = "object"
            this.interfaceList.addAll(interfaceList)
            this.fieldDefMap.putAll(fieldDefMap)
        }

        void extend(ExtendObjectDefinition extendObjectDef, Map<String, GraphQLTypeDefinition> allTypeDefMap) {
            // Extend interface first, then field.
            for (MNode extendObjectNode in extendObjectDef.extendObjectNodeList) {
                for (MNode childNode in extendObjectNode.children("interface")) {
                    GraphQLTypeDefinition interfaceTypeDef = allTypeDefMap.get(childNode.attribute("name"))
                    if (interfaceTypeDef == null)
                        throw new IllegalArgumentException("Extend object ${extendObjectDef.name}, but interface definition [${childNode.attribute("name")}] not found")
                    if (!(interfaceTypeDef instanceof InterfaceTypeDefinition))
                        throw new IllegalArgumentException("Extend object ${extendObjectDef.name}, but interface definition [${childNode.attribute("name")}] is not instance of InterfaceTypeDefinition")
                    extendInterface((InterfaceTypeDefinition) interfaceTypeDef, childNode)
                }
            }
            for (MNode extendObjectNode in extendObjectDef.extendObjectNodeList) {
                for (MNode childNode in extendObjectNode.children("field")) {
                    GraphQLSchemaUtil.mergeFieldDefinition(childNode, fieldDefMap, ecf)
                }
            }

            for (String excludeFieldName in extendObjectDef.excludeFields)
                fieldDefMap.remove(excludeFieldName)
        }

        List<FieldDefinition> getFieldList() {
            List<FieldDefinition> fieldList = new LinkedList<>()
            for (Map.Entry<String, FieldDefinition> entry in fieldDefMap)
                fieldList.add((FieldDefinition) entry.getValue())
            return fieldList
        }

        @Override
        List<String> getDependentTypes() {
            List<String> typeList = new LinkedList<>()
            for (String interfaceTypeName in interfaceList) typeList.add(interfaceTypeName)
            for (Map.Entry<String, FieldDefinition> entry in fieldDefMap) typeList.add(((FieldDefinition) entry.getValue()).type)

            return typeList
        }

        private void extendInterface(InterfaceTypeDefinition interfaceTypeDefinition, MNode interfaceNode) {
            for (Map.Entry<String, FieldDefinition> entry in interfaceTypeDefinition.fieldDefMap) {
                // Already use interface field.
                fieldDefMap.put(entry.getKey(), entry.getValue())
            }
            interfaceTypeDefinition.addResolver(interfaceNode.attribute("resolver-value"), name)
            logger.info("Object ${name} extending interface ${interfaceTypeDefinition.name}")
            if (!interfaceList.contains(interfaceTypeDefinition.name)) interfaceList.add(interfaceTypeDefinition.name)
        }
    }

    static class ExtendObjectDefinition {
        @SuppressWarnings("GrFinalVariableAccess")
        final ExecutionContextFactory ecf
        List<MNode> extendObjectNodeList = new ArrayList<>(1)
        String name, resolverField

        List<String> interfaceList = new LinkedList<>()
        Map<String, FieldDefinition> fieldDefMap = new LinkedHashMap<>()
        List<String> excludeFields = new ArrayList<>()
        Map<String, String> resolverMap = new LinkedHashMap<>()

        boolean convertToInterface = false

        ExtendObjectDefinition(MNode node, ExecutionContextFactory ecf) {
            this.ecf = ecf
            this.extendObjectNodeList.add(node)
            this.name = node.attribute("name")

            for (MNode childNode in node.children) {
                switch (childNode.name) {
                    case "interface":
                        interfaceList.add(childNode.attribute("name"))
                        break
                    case "field":
                        fieldDefMap.put(childNode.attribute("name"), new FieldDefinition(childNode, ecf))
                        break
                    case "exclude-field":
                        excludeFields.add(childNode.attribute("name"))
                        break
                    case "convert-to-interface":
                        convertToInterface = true
                        resolverField = childNode.attribute("resolver-field")
                        for (MNode resolverMapNode in childNode.children("resolver-map")) {
                            resolverMap.put(resolverMapNode.attribute("resolver-value"), resolverMapNode.attribute("resolver-type"))
                        }
                        break
                }
            }
        }

        ExtendObjectDefinition merge(ExtendObjectDefinition other) {
            extendObjectNodeList.addAll(other.extendObjectNodeList)
            resolverField = resolverField ?: other.resolverField
            interfaceList.addAll(other.interfaceList)
            fieldDefMap.putAll(other.fieldDefMap)
            excludeFields.addAll(other.excludeFields)
            resolverMap.putAll(other.resolverMap)
            convertToInterface = convertToInterface ?: other.convertToInterface
            return this
        }
    }

    static class AutoArgumentsDefinition {
        String entityName, include, required
        List<String> excludes = new LinkedList<>()

        AutoArgumentsDefinition(MNode node) {
            this.entityName = node.attribute("entity-name")
            this.include = node.attribute("include") ?: "all"
            this.required = node.attribute("required") ?: "false"
            for (MNode childNode in node.children("exclude")) {
                excludes.add(childNode.attribute("field-name"))
            }
        }
    }

    static class ArgumentDefinition implements Cloneable {
        String name
        Map<String, String> attributeMap = new LinkedHashMap<>()

        ArgumentDefinition(MNode node, FieldDefinition fieldDef) {
            this.name = node.attribute("name")
            attributeMap.put("type", node.attribute("type"))
            attributeMap.put("required", node.attribute("required") ?: "false")
            attributeMap.put("defaultValue", node.attribute("default-value"))

            for (MNode childNode in node.children) {
                if ("description".equals(childNode.name)) {
                    attributeMap.put("description", node.attribute("description"))
                }
            }
        }

        ArgumentDefinition(FieldDefinition fieldDef, String name, Map<String, String> attributeMap) {
            this.name = name
            this.attributeMap.putAll(attributeMap)
        }

        ArgumentDefinition(FieldDefinition fieldDef, String name, String type, String required, String defaultValue, String description) {
            this.name = name
            attributeMap.put("type", type)
            attributeMap.put("required", required)
            attributeMap.put("defaultValue", defaultValue)
            attributeMap.put("description", description)
        }

        String getName() { return name }
        String getType() { return attributeMap.get("type") }
        String getRequired() { return attributeMap.get("required") }
        String getDefaultValue() { return attributeMap.get("defaultValue") }
        String getDescription() { return attributeMap.get("description") }

        @Override
        ArgumentDefinition clone() {
            return new ArgumentDefinition(null, this.name, this.attributeMap)
        }
    }

    static class FieldDefinition implements Cloneable {
        ExecutionContextFactory ecf
        String name, type, description, depreciationReason
        String nonNull, isList, listItemNonNull
        String requireAuthentication
        boolean isMutation = false

        BaseDataFetcher dataFetcher
        String preDataFetcher, postDataFetcher

//        List<ArgumentDefinition> argumentList = new LinkedList<>()
        Map<String, ArgumentDefinition> argumentDefMap = new LinkedHashMap<>()

        FieldDefinition(MNode node, ExecutionContextFactory ecf) {
            this.ecf = ecf
            this.name = node.attribute("name")
            this.type = node.attribute("type")
            this.description = node.attribute("description")
            this.nonNull = node.attribute("non-null") ?: "false"
            this.isList = node.attribute("is-list") ?: "false"
            this.listItemNonNull = node.attribute("list-item-non-null") ?: "false"
            this.requireAuthentication = node.attribute("require-authentication") ?: "true"
            this.isMutation = "mutation".equals(node.attribute("for"))

            String dataFetcherType = ""
            MNode dataFetcherNode = null

            for (MNode childNode in node.children) {
                switch (childNode.name) {
                    case "description":
                        this.description = childNode.text
                        break
                    case "depreciation-reason":
                        this.depreciationReason = childNode.text
                        break
                    case "auto-arguments":
                        mergeArgument(new AutoArgumentsDefinition(childNode))
                        break
                    case "argument":
                        String argTypeName = getArgumentTypeName(childNode.attribute("type"), this.isList)
                        ArgumentDefinition argDef = getCachedArgumentDefinition(childNode.attribute("name"), argTypeName, childNode.attribute("required"))
                        if (argDef == null) {
                            argDef = new ArgumentDefinition(childNode, this)
                            putCachedArgumentDefinition(argDef)
                        }
                        mergeArgument(argDef)
                        break
                    case "service-fetcher":
                        dataFetcherType = "service"
                        dataFetcherNode = childNode
                        this.dataFetcher = new ServiceDataFetcher(childNode, this, ecf)
                        break
                    case "entity-fetcher":
                        dataFetcherType = "entity"
                        dataFetcherNode = childNode
                        this.dataFetcher = new EntityBatchedDataFetcher(childNode, this, ecf)
                        break
                    case "interface-fetcher":
                        dataFetcherType = "interface"
                        dataFetcherNode = childNode
                        String refName = childNode.attribute("ref") ?: "NOT_EXIST"
                        MNode refNode = interfaceFetcherNodeMap.get(refName)
                        this.dataFetcher = new InterfaceBatchedDataFetcher(childNode, refNode, this, ecf)
                        break
                    case "es-fetcher":
                        dataFetcherType = "elastic-search"
                        dataFetcherNode = childNode
                        this.dataFetcher = new ElasticSearchDataFetcher(childNode, this, ecf)
                        break
                    case "empty-fetcher":
                        dataFetcherType = "empty"
                        dataFetcherNode = childNode
                        this.dataFetcher = new EmptyDataFetcher(childNode, this)
                        break
                    case "pre-fetcher":
                        this.preDataFetcher = childNode.text
                        break
                    case "post-fetcher":
                        this.postDataFetcher = childNode.text
                        break
                }
            }

            Map<String, String> keyMap = getDataFetcherKeyMap(dataFetcherNode)
            switch (dataFetcherType) {
                case "entity":
                case "interface":
                    addEntityAutoArguments(new ArrayList<String>(), keyMap)
                    addPeriodValidArguments()
                    updateArgumentDefs()
                    break
                case "service":
                    if (isMutation) addInputArgument()
                    else addServiceAutoArguments(dataFetcherNode, keyMap)
                    break
                case "elastic-search":
                    addElasticSearchAutoArguments(dataFetcherNode)
                    break
            }
        }

        FieldDefinition(ExecutionContextFactory ecf, String name, String type) {
            this(ecf, name, type, new HashMap<>(), null, new ArrayList<>())
        }

        FieldDefinition(ExecutionContextFactory ecf, String name, String type, Map<String, String> fieldPropertyMap) {
            this(ecf, name, type, fieldPropertyMap, null, new ArrayList<>())
        }

        // This constructor used by auto creation of master-detail field
        FieldDefinition(ExecutionContextFactory ecf, String name, String type, Map<String, String> fieldPropertyMap,
                        List<String> excludedFields) {
            this(ecf, name, type, fieldPropertyMap, null, excludedFields)
        }

        // This constructor used by auto creation of master-detail field
        FieldDefinition(ExecutionContextFactory ecf, String name, String type, Map<String, String> fieldPropertyMap,
                        BaseDataFetcher dataFetcher, List<String> excludedArguments) {
            this.ecf = ecf
            this.name = name
            this.type = type
            this.dataFetcher = dataFetcher

            this.nonNull = fieldPropertyMap.get("nonNull") ?: "false"
            this.isList = fieldPropertyMap.get("isList") ?: "false"
            this.listItemNonNull = fieldPropertyMap.get("listItemNonNull") ?: "false"
            this.requireAuthentication = fieldPropertyMap.get("requireAuthentication") ?: "true"

            this.description = fieldPropertyMap.get("description")
            this.depreciationReason = fieldPropertyMap.get("depreciationReason")

            addEntityAutoArguments(excludedArguments, [:])
            updateArgumentDefs()
            addInputArgument()
            addPeriodValidArguments()
        }

        List<ArgumentDefinition> getArgumentList() {
            List<ArgumentDefinition> argumentList = new LinkedList<>()
            for (Map.Entry<String, ArgumentDefinition> entry in argumentDefMap) {
                argumentList.add(entry.getValue())
            }
            return argumentList
        }

        @Override
        FieldDefinition clone() {
            FieldDefinition other = new FieldDefinition(this.ecf, this.name, this.type)
            other.description = description
            other.depreciationReason = depreciationReason
            other.nonNull = nonNull
            other.isList = isList
            other.listItemNonNull = listItemNonNull
            other.requireAuthentication = requireAuthentication
            other.dataFetcher = dataFetcher
            other.preDataFetcher = preDataFetcher
            other.postDataFetcher = postDataFetcher

            List<ArgumentDefinition> otherArgumentList = new LinkedList<>()
            for (ArgumentDefinition argDef in argumentList) otherArgumentList.add(argDef.clone())
            other.argumentList.addAll(otherArgumentList)

            return other
        }

        private void addInputArgument() {
            if (!isMutation) return

            String inputTypeName = GraphQLSchemaUtil.camelCaseToUpperCamel(this.name) + "Input"
            ArgumentDefinition inputArgDef = new ArgumentDefinition(this, "input", inputTypeName, "true", null, "")
            argumentDefMap.put("input", inputArgDef)
        }

        private void updateArgumentDefs() {
            for (ArgumentDefinition argumentNode in argumentList) {
                // nothing
            }
        }

        void setDataFetcher(BaseDataFetcher dataFetcher) {
            this.dataFetcher = dataFetcher
        }

        void mergeArgument(ArgumentDefinition argumentDef) {
            mergeArgument(argumentDef.name, argumentDef.attributeMap)
        }

        void mergeArgument(AutoArgumentsDefinition autoArgumentsDef) {
            String entityName = autoArgumentsDef.entityName
            if (entityName == null || entityName.isEmpty())
                throw new IllegalArgumentException("Error in auto-arguments in field ${this.name}, no auto-arguments.@entity-name")

            EntityDefinition ed = ((ExecutionContextFactoryImpl) ecf).entityFacade.getEntityDefinition(entityName)
            if (ed == null) throw new IllegalArgumentException("Error in auto-arguments in field ${this.name}, the entity-name is not a valid entity name")

            String includeStr = autoArgumentsDef.include
            for (String fieldName in ed.getFieldNames("all".equals(includeStr) || "pk".equals(includeStr), "all".equals(includeStr) || "nonpk".equals(includeStr))) {
                if (autoArgumentsDef.excludes.contains(fieldName)) continue

                Map<String, String> map = new HashMap<>(4)
                map.put("type", getArgumentTypeName(GraphQLSchemaUtil.getEntityFieldGraphQLType(ed.getFieldInfo(fieldName).type), isList))
                map.put("required", autoArgumentsDef.required)
                map.put("defaultValue", "")
                map.put("description", "")
                mergeArgument(fieldName, map)
            }
        }

        ArgumentDefinition mergeArgument(final String argumentName, Map<String, String> attributeMap) {
            ArgumentDefinition baseArgumentDef = argumentDefMap.get(argumentName)
            if (baseArgumentDef == null) {
                baseArgumentDef = getCachedArgumentDefinition(argumentName, attributeMap.get("type"), attributeMap.get("required"))
                if (baseArgumentDef == null) {
                    baseArgumentDef = new ArgumentDefinition(this, argumentName, attributeMap)
                    putCachedArgumentDefinition(baseArgumentDef)
                }
                argumentDefMap.put(argumentName, baseArgumentDef)
            } else {
                baseArgumentDef.attributeMap.putAll(attributeMap)
            }
            return baseArgumentDef
        }

        private static Map<String, String> getDataFetcherKeyMap(MNode fetcherNode) {
            Map<String, String> keyMap = new HashMap<>(1)
            if (fetcherNode == null) return keyMap
            for (MNode keyMapNode in fetcherNode.children("key-map"))
                keyMap.put(keyMapNode.attribute("field-name"), keyMapNode.attribute("related") ?: keyMapNode.attribute("field-name"))
            return keyMap
        }

        private void addServiceAutoArguments(MNode serviceFetcherNode, Map<String, String> keyMap) {
            if (isMutation) return

            String serviceName = serviceFetcherNode.attribute("service")

            ServiceDefinition sd = ((ExecutionContextFactoryImpl) ecf).serviceFacade.getServiceDefinition(serviceName)
            if (sd == null) throw new IllegalArgumentException("Service ${serviceName} for field ${name} not found")

            for (String paramName in sd.getInParameterNames()) {
                MNode parameterNode = sd.getInParameter(paramName)
                String paramType = parameterNode.attribute("type") ?: "String"
                if (keyMap.values().contains(paramName)) continue
                if (paramType == "graphql.schema.DataFetchingEnvironment") continue // ignored

                // TODO: get description from parameter description node
                String paramDescription = ""

                String argType
                switch (paramType) {
                    case "com.moqui.graphql.OperationInputType": argType = "OperationInputType"; break
                    case "com.moqui.graphql.DateRangeInputType": argType = "DateRangeInputType"; break
                    case "com.moqui.graphql.PaginationInputType": argType = "PaginationInputType"; break
                    default:
                        argType = GraphQLSchemaUtil.javaTypeGraphQLMap.get(paramType)
                        break
                }
                if (!argType) throw new IllegalArgumentException("Parameter ${paramName} type ${paramType} can't be mapped")

                ArgumentDefinition argumentDef = getCachedArgumentDefinition(paramName, argType, null)
                if (argumentDef == null) {
                    argumentDef = new ArgumentDefinition(this, paramName, argType, null, null, paramDescription)
                    putCachedArgumentDefinition(argumentDef)
                }
                argumentDefMap.put(paramName, argumentDef)
            }
        }

        private void addElasticSearchAutoArguments(MNode elasticSearchFetcherNode) {
            if (isMutation) return
            ArgumentDefinition argumentDef
            if (isList == "true") {
                String queryParamName = elasticSearchFetcherNode.attribute("query-format") == "json" ? "queryJson" : "queryString"
                // add queryString argument
                argumentDef = getCachedArgumentDefinition(queryParamName, "String", "true")
                if (argumentDef == null) {
                    argumentDef = new ArgumentDefinition(this, queryParamName, "String", "true", null, null)
                    putCachedArgumentDefinition(argumentDef)
                }
                argumentDefMap.put(queryParamName, argumentDef)
            } else {
                argumentDef = getCachedArgumentDefinition("_id", "String", "true")
                if (argumentDef == null) {
                    argumentDef = new ArgumentDefinition(this, "_id", "String", "true", null, null)
                    putCachedArgumentDefinition(argumentDef)
                }
                argumentDefMap.put("_id", argumentDef)
            }
        }

        private void addEntityAutoArguments(List<String> excludedFields, Map<String, String> explicitKeyMap) {
            if (isMutation) return
            if (GraphQLSchemaUtil.graphQLScalarTypes.keySet().contains(type) || graphQLDirectiveArgumentMap.keySet().contains(type)) return

            if (!(((ExecutionContextFactoryImpl) ecf).entityFacade.isEntityDefined(type))) return

            EntityDefinition ed = ((ExecutionContextFactoryImpl) ecf).entityFacade.getEntityDefinition(type)

            List<String> fieldNames = new ArrayList<>()
            if ("true".equals(isList)) fieldNames.addAll(ed.getFieldNames(true, true))
            else fieldNames.addAll(ed.getFieldNames(true, false))

            fieldNames.removeAll(explicitKeyMap.values())

            for (String fieldName in fieldNames) {
                if (excludedFields.contains(fieldName)) continue
                FieldInfo fi = ed.getFieldInfo(fieldName)

                String fieldDescription = ""
                for (MNode descriptionMNode in fi.fieldNode.children("description"))
                    fieldDescription = fieldDescription + descriptionMNode.text + "\n"

                // Add fields in entity as argument
                String argType = getArgumentTypeName(GraphQLSchemaUtil.fieldTypeGraphQLMap.get(fi.type), isList)

                ArgumentDefinition argumentDef = getCachedArgumentDefinition(fi.name, argType, null)
                if (argumentDef == null) {
                    argumentDef = new ArgumentDefinition(this, fi.name, argType, null, null, fieldDescription)
                    putCachedArgumentDefinition(argumentDef)
                }
                argumentDefMap.put(fi.name, argumentDef)
            }
        }

        private void addPeriodValidArguments() {
            if (!"true".equals(isList)) return

            List<String> allArguments = argumentDefMap.keySet().toList()
            List<String> fromDateArguments = allArguments.findAll { String argument ->
                argument == "fromDate" || argument.endsWith("FromDate") }
            List<String> pairedFromDateArguments = fromDateArguments.findAll { String argument ->
                (argument == "fromDate" && allArguments.contains("thruDate")) ||
                allArguments.contains(argument.replace("FromDate", "ThruDate"))
            }
            for (String argument in pairedFromDateArguments) {
                String periodValidArgName = argument == "fromDate" ? "periodValid_"
                        : argument.replace("FromDate", "PeriodValid_")
                ArgumentDefinition argumentDef = getCachedArgumentDefinition(periodValidArgName, "Boolean", null)
                if (argumentDef == null) {
                    argumentDef = new ArgumentDefinition(this, periodValidArgName, "Boolean", null, null, "")
                    putCachedArgumentDefinition(argumentDef)
                }
                argumentDefMap.put(periodValidArgName, argumentDef)
            }
        }
    }

    static class InputObjectFieldDefinition {
        String name, type, description
        String nonNull, isList, listItemNonNull
        Object defaultValue

        InputObjectFieldDefinition(String name, String type, Object defaultValue, String description) {
            this.name = name
            this.type = type
            this.defaultValue = defaultValue
            this.nonNull = "false"
            this.isList = "false"
            this.listItemNonNull = "false"
            this.description = description
        }
        
        InputObjectFieldDefinition(String name, String type, Object defaultValue, String description,
                                   String nonNull, String isList, String listItemNonNull) {
            this.name = name
            this.type = type
            this.defaultValue = defaultValue
            this.nonNull = nonNull
            this.isList = isList
            this.listItemNonNull = listItemNonNull
            this.description = description
        }
    }
}
