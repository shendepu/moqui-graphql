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

import com.moqui.graphql.DataFetchingException
import com.moqui.impl.util.GraphQLSchemaUtil
import graphql.schema.DataFetcher
import graphql.schema.DataFetchingEnvironment
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
import graphql.schema.TypeResolver
import org.moqui.context.ArtifactAuthorizationException
import org.moqui.context.ArtifactTarpitException
import org.moqui.context.AuthenticationRequiredException
import org.moqui.context.ExecutionContext
import org.moqui.context.ExecutionContextFactory
import org.moqui.entity.EntityCondition
import org.moqui.entity.EntityFind
import org.moqui.entity.EntityList
import org.moqui.entity.EntityValue
import org.moqui.impl.context.ExecutionContextFactoryImpl
import org.moqui.impl.context.ExecutionContextImpl
import org.moqui.impl.context.UserFacadeImpl
import org.moqui.impl.entity.EntityDefinition
import org.moqui.impl.entity.FieldInfo
import org.moqui.impl.service.ServiceDefinition
import org.moqui.impl.service.ServiceFacadeImpl
import org.moqui.impl.webapp.ScreenResourceNotFoundException
import org.moqui.service.ServiceFacade
import org.moqui.util.MNode
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static graphql.Scalars.GraphQLBigDecimal
import static graphql.Scalars.GraphQLBigInteger
import static graphql.Scalars.GraphQLBoolean
import static graphql.Scalars.GraphQLByte
import static graphql.Scalars.GraphQLChar
import static graphql.Scalars.GraphQLFloat
import static graphql.Scalars.GraphQLID
import static graphql.Scalars.GraphQLInt
import static graphql.Scalars.GraphQLLong
import static graphql.Scalars.GraphQLShort
import static graphql.Scalars.GraphQLString
import static com.moqui.graphql.Scalars.GraphQLTimestamp


public class GraphQLSchemaDefinition {
    protected final static Logger logger = LoggerFactory.getLogger(GraphQLSchemaDefinition.class)

    @SuppressWarnings("GrFinalVariableAccess")
    protected final ServiceFacade sf
    @SuppressWarnings("GrFinalVariableAccess")
    protected final ExecutionContextFactory ecf
    @SuppressWarnings("GrFinalVariableAccess")
    protected final MNode schemaNode

    @SuppressWarnings("GrFinalVariableAccess")
    public final String schemaName
    @SuppressWarnings("GrFinalVariableAccess")
    protected final String queryType
    @SuppressWarnings("GrFinalVariableAccess")
    protected final String mutationType

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
    protected final List<String> preLoadObjectTypes = new LinkedList<>()

    protected Map<String, GraphQLTypeDefinition> allTypeDefMap = new LinkedHashMap<>()
    protected LinkedList<GraphQLTypeDefinition> allTypeDefSortedList = new LinkedList<>()
    protected Map<String, GraphQLTypeDefinition> requiredTypeDefMap = new LinkedHashMap<>()

    // Only cache scalar field definition
    protected static Map<String, FieldDefinition> fieldDefMap = new HashMap<>()

    // Cache argument of scalar and OperationInputType, DateRageInputType
    protected static Map<String, ArgumentDefinition> argumentDefMap = new HashMap<>()

//    protected Map<String, UnionTypeDefinition> unionTypeDefMap = new HashMap<>()
//    protected Map<String, EnumTypeDefinition> enumTypeDefMap = new HashMap<>()
    protected Map<String, InterfaceTypeDefinition> interfaceTypeDefMap = new LinkedHashMap<>()
//    protected Map<String, ObjectTypeDefinition> objectTypeDefMap = new HashMap<>()
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

    protected Integer graphQLEnumTypeCount = 0
    protected Integer graphQLUnionTypeCount = 0
    protected Integer graphQLInterfaceTypeCount = 0
    protected Integer graphQLObjectTypeCount = 0
    protected Integer graphQLFieldCount = 0
    protected Integer graphQLArgumentCount = 1
    protected Integer graphQLInputTypeCount = 0
    protected Integer graphQLInputFieldCount = 0

    protected static final String KEY_SPLITTER = "__"
    protected static final String NON_NULL_SUFFIX = "_1"
    protected static final String IS_LIST_SUFFIX = "_2"
    protected static final String LIST_ITEM_NON_NULL_SUFFIX = "_3"
    protected static final String REQUIRED_SUFFIX = "_a"

    static {
        createPredefinedGraphQLTypes()
    }


    public static final Map<String, GraphQLScalarType> graphQLScalarTypes = [
            "Int"       : GraphQLInt,           "Long"      : GraphQLLong,
            "Float"     : GraphQLFloat,         "String"    : GraphQLString,
            "Boolean"   : GraphQLBoolean,       "ID"        : GraphQLID,
            "BigInteger": GraphQLBigInteger,    "BigDecimal": GraphQLBigDecimal,
            "Byte"      : GraphQLByte,          "Short"     : GraphQLShort,
            "Char"      : GraphQLChar,          "Timestamp" : GraphQLTimestamp]

    static final List<String> graphQLStringTypes = ["String", "ID", "Char"]
    static final List<String> graphQLDateTypes = ["Timestamp"]
    static final List<String> graphQLNumericTypes = ["Int", "Long", "Float", "BigInteger", "BigDecimal", "Short"]
    static final List<String> graphQLBoolTypes = ["Boolean"]

    private static void createPredefinedGraphQLTypes() {
        // Add default GraphQLScalarType
        for (Map.Entry<String, GraphQLScalarType> entry in graphQLScalarTypes.entrySet()) {
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

    public GraphQLSchemaDefinition(ExecutionContextFactory ecf, MNode schemaNode) {
        this.ecf = ecf
        this.schemaNode = schemaNode

        this.schemaName = schemaNode.attribute("name")
        this.queryType = schemaNode.attribute("query")
        this.mutationType = schemaNode.attribute("mutation")

        GraphQLSchemaUtil.createObjectTypeNodeForAllEntities(ecf.getExecutionContext(), allTypeDefMap)

        for (MNode childNode in schemaNode.children) {
            switch (childNode.name) {
                case "input-type":
                    schemaInputTypeNameList.add(childNode.attribute("name"))
                    break
                case "interface":
                    InterfaceTypeDefinition interfaceTypeDef = new InterfaceTypeDefinition(childNode, ecf.getExecutionContext())
                    allTypeDefMap.put(childNode.attribute("name"), interfaceTypeDef)
                    interfaceTypeDefMap.put(childNode.attribute("name"), interfaceTypeDef)
                    break
                case "object":
                    allTypeDefMap.put(childNode.attribute("name"), new ObjectTypeDefinition(childNode, ecf.getExecutionContext()))
                    break
                case "union":
                    allTypeDefMap.put(childNode.attribute("name"), new UnionTypeDefinition(childNode))
                    break
                case "enum":
                    allTypeDefMap.put(childNode.attribute("name"), new EnumTypeDefinition(childNode))
                    break
                case "extend-object":
                    extendObjectDefMap.put(childNode.attribute("name"), new ExtendObjectDefinition(childNode))
                    break
                case "pre-load-object":
                    preLoadObjectTypes.add(childNode.attribute("name"))
                    break
            }
        }

        updateAllTypeDefMap()
    }

    public static FieldDefinition getCachedFieldDefinition(String name, String rawTypeName, String nonNull, String isList, String listItemNonNull) {
        return fieldDefMap.get(getFieldKey(name, rawTypeName, nonNull, isList, listItemNonNull))
    }

    public static void putCachedFieldDefinition(FieldDefinition fieldDef) {
        String fieldKey = getFieldKey(fieldDef.name, fieldDef.type, fieldDef.nonNull, fieldDef.isList, fieldDef.listItemNonNull)
        if (fieldDefMap.get(fieldKey) != null)
            throw new IllegalArgumentException("FieldDefinition [${fieldDef.name} - ${fieldDef.type}] already exists in cache")
        fieldDefMap.put(fieldKey, fieldDef)
    }

    public static ArgumentDefinition getCachedArgumentDefinition(String name, String type, String required) {
        return argumentDefMap.get(getArgumentKey(name, type, required))
    }

    public static String getArgumentTypeName(String type, String fieldIsList) {
        if (!"true".equals(fieldIsList)) return type
        if (graphQLStringTypes.contains(type) || graphQLNumericTypes.contains(type) || graphQLDateTypes.contains(type))
            return operationInputType.name
        if (graphQLDateTypes.contains(type)) return dateRangeInputType.name

        return type
    }

    public static void putCachedArgumentDefinition(ArgumentDefinition argDef) {
        if (!(graphQLScalarTypes.containsKey(argDef.type) ||
                dateRangeInputType.name.equals(argDef.type) ||
                operationInputType.name.equals(argDef.type))) return

        String argumentKey = getArgumentKey(argDef.name, argDef.type, argDef.required)
        if (argumentDefMap.get(argumentKey) != null)
            throw new IllegalArgumentException("ArgumentDefinition [${argDef.name} - ${argDef.type}] already exists in cache")
        argumentDefMap.put(argumentKey, argDef)
    }

    public static String getArgumentKey(String name, String type) {
        return getArgumentKey(name, type, null)
    }

    public static String getArgumentKey(String name, String type, String required) {
        String argumentKey = name + KEY_SPLITTER + type
        if ("true".equals(required)) argumentKey = argumentKey + REQUIRED_SUFFIX
        return argumentKey
    }

    public static void clearAllCachedGraphQLTypes() {
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

        createPredefinedGraphQLTypes()
    }

    private GraphQLTypeDefinition getTypeDef(String name) {
        return allTypeDefMap.get(name)
//        return allTypeNodeList.find({ name.equals(it.name) })
    }

    private void addSchemaInputTypes() {
        // Add default GraphQLScalarType
        for (Map.Entry<String, GraphQLScalarType> entry in graphQLScalarTypes.entrySet()) {
            schemaInputTypeMap.put(entry.getKey(), entry.getValue())
        }

        schemaInputTypeMap.put(paginationInputType.name, paginationInputType)
        schemaInputTypeMap.put(operationInputType.name, operationInputType)
        schemaInputTypeMap.put(dateRangeInputType.name, dateRangeInputType)

        // Add explicitly defined input types from *.graphql.xml
        for (String inputTypeName in schemaInputTypeNameList) {
            GraphQLType type = graphQLInputTypeMap.get(inputTypeName)
            if (type == null)
                throw new IllegalArgumentException("GraphQLInputType [${inputTypeName}] for schema [${this.schemaName}] not found")
            schemaInputTypeMap.put(inputTypeName, type)
        }

        addSchemaInputObjectTypes()
    }

    // Create InputObjectType (Input) for mutation fields
    private void addSchemaInputObjectTypes() {

        for (Map.Entry<String, GraphQLTypeDefinition> entry in allTypeDefMap) {
            if (!(entry.getValue() instanceof ObjectTypeDefinition)) continue
            for (FieldDefinition fieldDef in ((ObjectTypeDefinition) entry.getValue()).fieldList) {
                if (!fieldDef.isMutation) continue
                if (fieldDef.dataFetcher == null)
                    throw new IllegalArgumentException("FieldDefinition [${fieldDef.name} - ${fieldDef.type}] as mutation must have a data fetcher")
                if (fieldDef.dataFetcher instanceof EmptyDataFetcher)
                    throw new IllegalArgumentException("FieldDefinition [${fieldDef.name} - ${fieldDef.type}] as mutation can't have empty data fetcher")

                if (fieldDef.dataFetcher instanceof DataFetcherService) {
                    String serviceName = ((DataFetcherService) fieldDef.dataFetcher).serviceName
                    String inputTypeName = GraphQLSchemaUtil.camelCaseToUpperCamel(fieldDef.name) + "Input"
                    ServiceDefinition sd = ((ExecutionContextFactoryImpl) ecf).serviceFacade.getServiceDefinition(serviceName)

                    logger.info("======== inputTypeName - ${inputTypeName}")
                    Map<String, InputObjectFieldDefinition> inputFieldMap = new LinkedHashMap<>(sd.getInParameterNames().size())
                    for (String parmName in sd.getInParameterNames()) {
                        MNode parmNode = sd.getInParameter(parmName)
                        String inputFieldType = GraphQLSchemaUtil.getGraphQLType(parmNode.attribute("type"))
                        logger.info("======== inputField ${parmName} - SD type: ${parmNode.attribute("type")}, inputFieldType: ${inputFieldType}")
                        Object defaultValue = null

                        InputObjectFieldDefinition inputFieldDef = new InputObjectFieldDefinition(parmName, inputFieldType, defaultValue, "")
                        inputFieldMap.put(parmName, inputFieldDef)
                    }
                    GraphQLInputObjectType.Builder inputObjectTypeBuilder = GraphQLInputObjectType.newInputObject()
                            .name(inputTypeName).description("Autogenerated input type of ${inputTypeName}")

                    for (Map.Entry<String, InputObjectFieldDefinition> inputFieldEntry in inputFieldMap) {
                        InputObjectFieldDefinition inputFieldDef = inputFieldEntry.getValue()

                        if ("clientMutationId".equals(inputFieldDef.name)) continue
                        if (!graphQLScalarTypes.keySet().contains(inputFieldDef.type))
                            throw new IllegalArgumentException("GraphQLInputObjectField [${inputFieldDef.name} - ${inputFieldDef.type}] should be GraphQLScalarType types")

                        String inputFieldKey = inputFieldDef.name + KEY_SPLITTER + inputFieldDef.type
                        GraphQLInputObjectField inputField = graphQLInputObjectFieldMap.get(inputFieldKey)
                        if (inputField == null) {
                            inputField = GraphQLInputObjectField.newInputObjectField()
                                    .name(inputFieldDef.name)
                                    .type(graphQLInputTypeMap.get(inputFieldDef.type))
                                    .defaultValue(inputFieldDef.defaultValue)
                                    .description(inputFieldDef.description)
                                    .build()
                        }
                        inputObjectTypeBuilder.field(inputField)
                    }
                    inputObjectTypeBuilder.field(clientMutationIdInputField)
                    GraphQLInputObjectType inputObjectType = inputObjectTypeBuilder.build()
                    graphQLInputTypeMap.put(inputTypeName, inputObjectType)
                }
            }
        }
    }

    private void populateSortedTypes() {
        allTypeDefSortedList.clear()

        GraphQLTypeDefinition queryTypeDef = getTypeDef(queryType)
        GraphQLTypeDefinition mutationTypeDef = getTypeDef(mutationType)

        if (queryTypeDef == null) {
            logger.error("No query type [${queryType}] defined for GraphQL schema")
            return
        }

        for (String preLoadObjectType in preLoadObjectTypes) {
            GraphQLTypeDefinition preLoadObjectTypeDef = getTypeDef(preLoadObjectType)
            if (preLoadObjectTypeDef != null) allTypeDefSortedList.add(preLoadObjectTypeDef)
        }

        TreeNode<GraphQLTypeDefinition> rootNode = new TreeNode<>(null)
        TreeNode<GraphQLTypeDefinition> interfaceNode = new TreeNode<>(null)

        for (Map.Entry<String, InterfaceTypeDefinition> entry in interfaceTypeDefMap)
            interfaceNode.children.add(new TreeNode<GraphQLTypeDefinition>((InterfaceTypeDefinition) entry.getValue()))

        TreeNode<GraphQLTypeDefinition> queryTypeNode = new TreeNode<GraphQLTypeDefinition>(queryTypeDef)
        rootNode.children.add(queryTypeNode)

        List<String> objectTypeNames = [queryType, mutationType]

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
                if (graphQLScalarTypes.containsKey(type)) continue
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

    public GraphQLSchema getSchema() {
        addSchemaInputTypes()
        populateSortedTypes()

        Integer unionTypeCount = 0, enumTypeCount = 0, interfaceTypeCount = 0, objectTypeCount = 0

        // Initialize interface type first to prevent null reference when initialize object type
//        for (GraphQLTypeDefinition typeDef in allTypeDefSortedList) {
//            if (!("interface".equals(typeDef.type))) continue
//            addGraphQLInterfaceType((InterfaceTypeDefinition) typeDef)
//            interfaceTypeCount++
//        }

        for (GraphQLTypeDefinition typeDef in allTypeDefSortedList) {
            switch (typeDef.type) {
                case "union":
                    addGraphQLUnionType((UnionTypeDefinition) typeDef)
                    unionTypeCount++
                    break
                case "enum":
                    addGraphQLEnumType((EnumTypeDefinition) typeDef)
                    enumTypeCount++
                    break
                case "interface":
                    addGraphQLInterfaceType((InterfaceTypeDefinition) typeDef)
                    interfaceTypeCount++
                    break
                case "object":
                    addGraphQLObjectType((ObjectTypeDefinition) typeDef)
                    objectTypeCount++
                    break
            }
        }

        // Create GraphQLSchema
        GraphQLObjectType schemaQueryType = graphQLObjectTypeMap.get(this.queryType)
        if (schemaQueryType == null)
            throw new IllegalArgumentException("GraphQLObjectType [${this.queryType}] as query type for schema [${this.schemaName}] not found")

        GraphQLSchema.Builder schemaBuilder = GraphQLSchema.newSchema().query(schemaQueryType)

        if (this.mutationType) {
            GraphQLObjectType schemaMutationType = graphQLObjectTypeMap.get(this.mutationType)
            if (schemaMutationType == null)
                throw new IllegalArgumentException("GraphQLObjectType [${this.mutationType}] as mutation type for schema [${this.schemaName}] not found")
            schemaBuilder = schemaBuilder.mutation(schemaMutationType)
        }

        GraphQLSchema schema = schemaBuilder.build(new HashSet<GraphQLType>(schemaInputTypeMap.values()))

        logger.info("Schema [${schemaName}] loaded: ${unionTypeCount} union type, ${enumTypeCount} enum type, ${interfaceTypeCount} interface type, ${objectTypeCount} object type")
        logger.info("Schema [${schemaName}] created: ${graphQLUnionTypeCount} union type, ${graphQLEnumTypeCount} enum type, " +
                "${graphQLInterfaceTypeCount} interface type, ${graphQLObjectTypeCount} object type, ${graphQLInputTypeCount} input type, " +
                "${graphQLInputFieldCount} input field, ${graphQLFieldCount} field, ${graphQLArgumentCount} argument")
        logger.info("Globally ${graphQLFieldMap.size()} fields, ${graphQLOutputTypeMap.size()} output types, ${graphQLInputTypeMap.size()} imput types")
        return schema
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

            InterfaceTypeDefinition interfaceTypeDef = new InterfaceTypeDefinition(objectTypeDef, extendObjectDef, ecf.getExecutionContext())
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
                graphQLTypeReferenceMap.put(rawTypeName, rawType)
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
            rawType = new GraphQLTypeReference(rawTypeName)
            graphQLTypeReferenceMap.put(rawTypeName, rawType)
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
                    String listItemNonNull, String description, DataFetcherHandler dataFetcherHandler) {
        GraphQLOutputType rawType = graphQLOutputTypeMap.get(rawTypeName)
//        if (rawType == null) throw new IllegalArgumentException("GraphQLOutputType [${rawTypeName}] for field [${name}] not found")
        if (rawType == null) {
            rawType = graphQLTypeReferenceMap.get(rawTypeName)
            if (rawType == null) {
                rawType = new GraphQLTypeReference(rawTypeName)
                graphQLTypeReferenceMap.put(rawTypeName, rawType)
            }
        }
        return getGraphQLFieldWithNoArgs(name, rawType, nonNull, isList, listItemNonNull, description, dataFetcherHandler)
    }

    private static GraphQLFieldDefinition getGraphQLFieldWithNoArgs(String name, GraphQLOutputType rawType, String nonNull, String isList,
                    String listItemNonNull, DataFetcherHandler dataFetcherHandler) {
        return getGraphQLFieldWithNoArgs(name, rawType, nonNull, isList, listItemNonNull, "", dataFetcherHandler)
    }

    private static GraphQLFieldDefinition getGraphQLFieldWithNoArgs(String name, GraphQLOutputType rawType, String nonNull, String isList,
                                                                    String listItemNonNull, String description, DataFetcherHandler dataFetcherHandler) {
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

        if (dataFetcherHandler != null) {
            fieldBuilder.dataFetcher(new DataFetcher() {
                @Override
                public Object get(DataFetchingEnvironment environment) {
                    return dataFetcherHandler.get(environment)
                }
            })
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

    private static GraphQLFieldDefinition buildSchemaField(FieldDefinition fieldDef) {
        GraphQLFieldDefinition graphQLFieldDef

        if (fieldDef.argumentList.size() == 0 && graphQLScalarTypes.containsKey(fieldDef.type))
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
            graphQLFieldDefBuilder.dataFetcher(new DataFetcher() {
                @Override
                public Object get(DataFetchingEnvironment environment) {
                    return fieldDef.dataFetcher.get(environment)
                }
            })
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

    public static class TreeNode<T> {
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
        final ExecutionContext ec

        String convertFromObjectTypeName

        String typeResolver
        Map<String, FieldDefinition> fieldDefMap = new LinkedHashMap<>()
        String resolverField
        Map<String, String> resolverMap = new LinkedHashMap<>()
        String defaultResolvedTypeName

        InterfaceTypeDefinition(MNode node, ExecutionContext ec) {
            this.ec = ec
            this.name = node.attribute("name")
            this.type = "interface"
            this.typeResolver = node.attribute("type-resolver")
            for (MNode childNode in node.children) {
                switch (childNode.name) {
                    case "description":
                        this.description = childNode.text
                        break
                    case "field":
                        fieldDefMap.put(childNode.attribute("name"), new FieldDefinition(childNode, ec))
//                        typeList.add(childNode.attribute("type"))
                        break
                }
            }
        }

        InterfaceTypeDefinition(ObjectTypeDefinition objectTypeDef, ExtendObjectDefinition extendObjectDef, ExecutionContext ec) {
            this.convertFromObjectTypeName = objectTypeDef.name
            this.ec = ec
            this.name = objectTypeDef.name + "Interface"
            this.type = "interface"
            this.defaultResolvedTypeName = objectTypeDef.name
            this.resolverField = extendObjectDef.resolverField
            this.resolverMap.putAll(extendObjectDef.resolverMap)

            fieldDefMap.putAll(objectTypeDef.fieldDefMap)
            for (MNode fieldNode in extendObjectDef.extendObjectNode.children("field")) {
                GraphQLSchemaUtil.mergeFieldDefinition(fieldNode, fieldDefMap, ec)
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
        final ExecutionContext ec

        String convertToInterface
        List<String> interfaceList = new LinkedList<>()
        Map<String, InterfaceTypeDefinition> interfacesMap
        Map<String, FieldDefinition> fieldDefMap = new LinkedHashMap<>()

        ObjectTypeDefinition(MNode node, ExecutionContext ec) {
            this.ec = ec
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
                        fieldDefMap.put(childNode.attribute("name"), new FieldDefinition(childNode, ec))
//                        typeList.add(childNode.attribute("type"))
                        break
                }
            }
        }

        ObjectTypeDefinition(ExecutionContext ec, String name, String description, List<String> interfaceList,
                             Map<String, FieldDefinition> fieldDefMap) {
            this.ec = ec
            this.name = name
            this.description = description
            this.type = "object"
            this.interfaceList.addAll(interfaceList)
            this.fieldDefMap.putAll(fieldDefMap)
        }

        public void extend(ExtendObjectDefinition extendObjectDef, Map<String, GraphQLTypeDefinition> allTypeDefMap) {
            // Extend interface first, then field.
            for (MNode childNode in extendObjectDef.extendObjectNode.children("interface")) {
                GraphQLTypeDefinition interfaceTypeDef = allTypeDefMap.get(childNode.attribute("name"))
                if (interfaceTypeDef == null)
                    throw new IllegalArgumentException("Interface definition [${childNode.attribute("name")}] not found")
                if (!(interfaceTypeDef instanceof InterfaceTypeDefinition))
                    throw new IllegalArgumentException("Interface definition [${childNode.attribute("name")}] is not instance of InterfaceTypeDefinition")
                extendInterface((InterfaceTypeDefinition) interfaceTypeDef, childNode)
            }
            for (MNode childNode in extendObjectDef.extendObjectNode.children("field")) {
                GraphQLSchemaUtil.mergeFieldDefinition(childNode, fieldDefMap, ec)
            }

            for (String excludeFieldName in extendObjectDef.excludeFields)
                fieldDefMap.remove(excludeFieldName)
        }

        public List<FieldDefinition> getFieldList() {
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
            interfaceTypeDefinition.addResolver(interfaceNode.attribute("resolver-value"), interfaceNode.attribute("resolver-type"))
            logger.info("Object ${name} extending interface ${interfaceTypeDefinition.name}")
            if (!interfaceList.contains(interfaceTypeDefinition.name)) interfaceList.add(interfaceTypeDefinition.name)
        }
    }

    static class ExtendObjectDefinition {
        @SuppressWarnings("GrFinalVariableAccess")
        final ExecutionContext ec
        MNode extendObjectNode
        String name, resolverField

        List<String> interfaceList = new LinkedList<>()
        Map<String, FieldDefinition> fieldDefMap = new LinkedHashMap<>()
        List<String> excludeFields = new ArrayList<>()
        Map<String, String> resolverMap = new LinkedHashMap<>()

        boolean convertToInterface = false

        ExtendObjectDefinition(MNode node) {
            this.extendObjectNode = node
            this.name = node.attribute("name")

            for (MNode childNode in node.children) {
                switch (childNode.name) {
                    case "interface":
                        interfaceList.add(childNode.attribute("name"))
                        break
                    case "field":
                        fieldDefMap.put(childNode.attribute("name"), new FieldDefinition(childNode, ec))
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

        public String getName() { return name }
        public String getType() { return attributeMap.get("type") }
        public String getRequired() { return attributeMap.get("required") }
        public String getDefaultValue() { return attributeMap.get("defaultValue") }
        public String getDescription() { return attributeMap.get("description") }

        @Override
        public ArgumentDefinition clone() {
            return new ArgumentDefinition(null, this.name, this.attributeMap)
        }
    }

    static class FieldDefinition implements Cloneable {
        ExecutionContext ec
        String name, type, description, depreciationReason
        String nonNull, isList, listItemNonNull
        String requireAuthentication
        boolean isMutation = false

        DataFetcherHandler dataFetcher
        String preDataFetcher, postDataFetcher

//        List<ArgumentDefinition> argumentList = new LinkedList<>()
        Map<String, ArgumentDefinition> argumentDefMap = new LinkedHashMap<>()

        FieldDefinition(MNode node, ExecutionContext ec) {
            this.ec = ec
            this.name = node.attribute("name")
            this.type = node.attribute("type")
            this.description = node.attribute("description")
            this.nonNull = node.attribute("non-null") ?: "false"
            this.isList = node.attribute("is-list") ?: "false"
            this.listItemNonNull = node.attribute("list-item-non-null") ?: "false"
            this.requireAuthentication = node.attribute("require-authentication") ?: "true"
            this.isMutation = "mutation".equals(node.attribute("for"))

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
                        this.dataFetcher = new DataFetcherService(childNode, this, ec)
                        break
                    case "entity-fetcher":
                        this.dataFetcher = new DataFetcherEntity(childNode, this, ec)
                        break
                    case "empty-fetcher":
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
            if (dataFetcher == null && !graphQLScalarTypes.keySet().contains(type))
                dataFetcher = new EmptyDataFetcher(this)

            addAutoArguments(new ArrayList<String>())
            updateArgumentDefs()
            addInputArgument()
        }

        FieldDefinition(ExecutionContext ec, String name, String type) {
            this(ec, name, type, new HashMap<>(), null, new ArrayList<>())
        }

        FieldDefinition(ExecutionContext ec, String name, String type, Map<String, String> fieldPropertyMap) {
            this(ec, name, type, fieldPropertyMap, null, new ArrayList<>())
        }

        // This constructor used by auto creation of master-detail field
        FieldDefinition(ExecutionContext ec, String name, String type, Map<String, String> fieldPropertyMap,
                        List<String> excludedFields) {
            this(ec, name, type, fieldPropertyMap, null, excludedFields)
        }

        // This constructor used by auto creation of master-detail field
        FieldDefinition(ExecutionContext ec, String name, String type, Map<String, String> fieldPropertyMap,
                        DataFetcherHandler dataFetcher, List<String> excludedArguments) {
            this.ec = ec
            this.name = name
            this.type = type
            this.dataFetcher = dataFetcher

            this.nonNull = fieldPropertyMap.get("nonNull") ?: "false"
            this.isList = fieldPropertyMap.get("isList") ?: "false"
            this.listItemNonNull = fieldPropertyMap.get("listItemNonNull") ?: "false"
            this.requireAuthentication = fieldPropertyMap.get("requireAuthentication") ?: "true"

            this.description = fieldPropertyMap.get("description")
            this.depreciationReason = fieldPropertyMap.get("depreciationReason")

            addAutoArguments(excludedArguments)
            updateArgumentDefs()
            addInputArgument()
        }

        public List<ArgumentDefinition> getArgumentList() {
            List<ArgumentDefinition> argumentList = new LinkedList<>()
            for (Map.Entry<String, ArgumentDefinition> entry in argumentDefMap) {
                argumentList.add(entry.getValue())
            }
            return argumentList
        }

        @Override
        public FieldDefinition clone() {
            FieldDefinition other = new FieldDefinition(this.ec, this.name, this.type)
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

        public void setDataFetcher(DataFetcherHandler dataFetcher) {
            this.dataFetcher = dataFetcher
        }

        public void mergeArgument(ArgumentDefinition argumentDef) {
            mergeArgument(argumentDef.name, argumentDef.attributeMap)
        }

        public void mergeArgument(AutoArgumentsDefinition autoArgumentsDef) {
            String entityName = autoArgumentsDef.entityName
            if (entityName == null || entityName.isEmpty())
                throw new IllegalArgumentException("Error in auto-arguments in field ${this.name}, no auto-arguments.@entity-name")
            ExecutionContextImpl eci = (ExecutionContextImpl) ec
            EntityDefinition ed = eci.getEntityFacade().getEntityDefinition(entityName)
            if (ed == null) throw new IllegalArgumentException("Error in auto-arguments in field ${this.name}, the entity-name is not a valid entity name")

            String includeStr = autoArgumentsDef.include
            for (String fieldName in ed.getFieldNames("all".equals(includeStr) || "pk".equals(includeStr), "all".equals(includeStr) || "nonpk".equals(includeStr))) {
                if (autoArgumentsDef.excludes.contains(fieldName)) continue

                Map<String, String> map = new HashMap<>(4)
                map.put("type", GraphQLSchemaUtil.getEntityFieldGraphQLType(ed.getFieldInfo(fieldName).type))
                map.put("required", autoArgumentsDef.required)
                map.put("defaultValue", "")
                map.put("description", "")
                mergeArgument(fieldName, map)
            }
        }

        public ArgumentDefinition mergeArgument(final String argumentName, Map<String, String> attributeMap) {
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

        private void addAutoArguments(List<String> excludedFields) {
            if (graphQLScalarTypes.keySet().contains(type) || graphQLDirectiveArgumentMap.keySet().contains(type)) return
            if (!((ExecutionContextImpl) ec).getEntityFacade().isEntityDefined(type)) return

            EntityDefinition ed = ((ExecutionContextImpl) ec).getEntityFacade().getEntityDefinition(type)

            List<String> fieldNames = new ArrayList<>()
            if ("true".equals(isList)) fieldNames.addAll(ed.getFieldNames(true, true))
            else fieldNames.addAll(ed.getFieldNames(true, false))

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
    }

    static class InputObjectFieldDefinition {
        String name, type, description
        Object defaultValue

        InputObjectFieldDefinition(String name, String type, Object defaultValue, String description) {
            this.name = name
            this.type = type
            this.defaultValue = defaultValue
            this.description = description
        }
    }

    static abstract class DataFetcherHandler {
        @SuppressWarnings("GrFinalVariableAccess")
        final ExecutionContext ec
        @SuppressWarnings("GrFinalVariableAccess")
        final FieldDefinition fieldDef

        DataFetcherHandler(FieldDefinition fieldDef, ExecutionContext ec) {
            this.ec = ec
            this.fieldDef = fieldDef
        }

        Object get(DataFetchingEnvironment environment) {
            try {
                return fetch(environment)
            } catch (AuthenticationRequiredException e) {
                throw new DataFetchingException('401', e.getMessage())
            } catch (ArtifactAuthorizationException e) {
                throw new DataFetchingException('403', e.getMessage())
            } catch (ScreenResourceNotFoundException e) {
                throw new DataFetchingException('404', e.getMessage())
            } catch (ArtifactTarpitException e) {
                throw new DataFetchingException('429', e.getMessage())
            } catch (DataFetchingException e) {
                throw e
            }
            catch (Throwable t) {
                throw new DataFetchingException("UNKNOWN", t.getMessage())
            }
        }

        Object fetch(DataFetchingEnvironment environment) { return null }
    }

    static class DataFetcherService extends DataFetcherHandler {
        String serviceName
        String requireAuthentication

        DataFetcherService(MNode node, FieldDefinition fieldDef, ExecutionContext ec) {
            super(fieldDef, ec)
            this.requireAuthentication = node.attribute("require-authentication") ?: fieldDef.requireAuthentication ?: "true"

            this.serviceName = node.attribute("service")

            ServiceDefinition sd = ((ExecutionContextImpl) ec).serviceFacade.getServiceDefinition(serviceName)
            if (sd == null) throw new IllegalArgumentException("Service ${serviceName} not found")
        }

        @Override
        Object fetch(DataFetchingEnvironment environment) {
            logger.info("---- running data fetcher service [${serviceName}] ...")
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
                logger.info("inputFieldsMap - ${inputFieldsMap}")

                Map result
                if (fieldDef.isMutation) {
                    result = ec.getService().sync().name(serviceName).parameters(inputFieldsMap).call()
                } else {
                    result = ec.getService().sync().name(serviceName)
                            .parameter("environment", environment)
                            .parameters(inputFieldsMap).call()
                }

                return result
            } finally {
                if (loggedInAnonymous) ((UserFacadeImpl) ec.getUser()).logoutAnonymousOnly()
            }
        }
    }

    static class DataFetcherEntity extends DataFetcherHandler {
        String entityName, interfaceEntityName, operation
        String requireAuthentication
        String interfaceEntityPkField
        List<String> pkFieldNames = new ArrayList<>(1)
        String fieldRawType
        Map<String, String> relKeyMap = new HashMap<>()

        DataFetcherEntity(MNode node, FieldDefinition fieldDef, ExecutionContext ec) {
            super(fieldDef, ec)

            Map<String, String> keyMap = new HashMap<>()
            for (MNode keyMapNode in node.children("key-map"))
                keyMap.put(keyMapNode.attribute("field-name"), keyMapNode.attribute("related") ?: keyMapNode.attribute("field-name"))

            initializeFields(node.attribute("entity-name"), node.attribute("interface-entity-name"), keyMap)
        }

        DataFetcherEntity(ExecutionContext ec, FieldDefinition fieldDef, String entityName, Map<String, String> relKeyMap) {
            this(ec, fieldDef, entityName, null, relKeyMap)
        }

        DataFetcherEntity(ExecutionContext ec, FieldDefinition fieldDef, String entityName, String interfaceEntityName, Map<String, String> relKeyMap) {
            super(fieldDef, ec)
            initializeFields(entityName, interfaceEntityName, relKeyMap)
        }

        private void initializeFields(String entityName, String interfaceEntityName, Map<String, String> relKeyMap) {
            this.requireAuthentication = fieldDef.requireAuthentication ?: "true"
            this.entityName = entityName
            this.interfaceEntityName = interfaceEntityName
            this.fieldRawType = fieldDef.type
            this.relKeyMap.putAll(relKeyMap)
            if ("true".equals(fieldDef.isList)) this.operation = "list"
            else this.operation = "one"

            if (interfaceEntityName) {
                EntityDefinition ed = ((ExecutionContextImpl) ec).getEntityFacade().getEntityDefinition(interfaceEntityName)
                if (ed.getFieldNames(true, false).size() != 1)
                    throw new IllegalArgumentException("Entity ${interfaceEntityName} for interface should have one primary key")
                interfaceEntityPkField = ed.getFieldNames(true, false).first()
            }

            EntityDefinition ed = ((ExecutionContextImpl) ec).getEntityFacade().getEntityDefinition(entityName)
            pkFieldNames.addAll(ed.pkFieldNames)
        }

        @Override
        Object fetch(DataFetchingEnvironment environment) {
            logger.info("---- running data fetcher entity for entity [${entityName}] with operation [${operation}] ...")
            logger.info("arguments  - ${environment.arguments}")
            logger.info("source     - ${environment.source}")
            logger.info("context    - ${environment.context}")
            logger.info("relKeyMap  - ${relKeyMap}")
            logger.info("interfaceEntityName    - ${interfaceEntityName}")

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
                    if (one == null) return  null
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
                    Map<String, Object> pageInfo = ['pageIndex'   : pageIndex, 'pageSize': pageSize, 'totalCount': count,
                            'pageMaxIndex': pageMaxIndex, 'pageRangeLow': pageRangeLow, 'pageRangeHigh': pageRangeHigh,
                            'hasPreviousPage': hasPreviousPage, 'hasNextPage': hasNextPage] as Map<String, Object>

                    EntityList el = ef.list()
                    List<Map<String, Object>> edgesDataList = new ArrayList(el.size())
                    Map<String, Object> edgesData
                    String cursor

                    if (el == null || el.size() == 0) {
                        // Do nothing
                    } else {
                        if (interfaceEntityName == null || interfaceEntityName.isEmpty() || entityName.equals(interfaceEntityName)) {
                            pageInfo.put("startCursor", GraphQLSchemaUtil.base64EncodeCursor(el.get(0), fieldRawType, pkFieldNames))
                            pageInfo.put("endCursor", GraphQLSchemaUtil.base64EncodeCursor(el.get(el.size() - 1), fieldRawType, pkFieldNames))
                            for (EntityValue ev in el) {
                                edgesData = new HashMap<>(2)
                                cursor = GraphQLSchemaUtil.base64EncodeCursor(ev, fieldRawType, pkFieldNames)
                                edgesData.put("cursor", cursor)
                                edgesData.put("node", ev.getPlainValueMap(0))
                                edgesDataList.add(edgesData)
                            }
                        } else {
                            List<Object> pkValues = new ArrayList<>()
                            for (EntityValue ev in el) pkValues.add(ev.get(interfaceEntityPkField))

                            EntityFind efInterface = ec.entity.find(interfaceEntityName).condition(interfaceEntityPkField, EntityCondition.ComparisonOperator.IN, pkValues)

                            Map<String, Object> jointOneMap, matchedOne

                            pageInfo.put("startCursor", GraphQLSchemaUtil.base64EncodeCursor(el.get(0), fieldRawType, pkFieldNames))
                            pageInfo.put("endCursor", GraphQLSchemaUtil.base64EncodeCursor(el.get(el.size() - 1), fieldRawType, pkFieldNames))
                            for (EntityValue ev in el) {
                                edgesData = new HashMap<>(2)
                                cursor = GraphQLSchemaUtil.base64EncodeCursor(ev, fieldRawType, pkFieldNames)
                                edgesData.put("cursor", cursor)
                                jointOneMap = ev.getPlainValueMap(0)
                                matchedOne = efInterface.list().find({ it.get(interfaceEntityPkField).equals(ev.get(interfaceEntityPkField)) })
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

    static class EmptyDataFetcher extends DataFetcherHandler {
        EmptyDataFetcher (MNode node, FieldDefinition fieldDef) {
            super(fieldDef, null)
        }

        EmptyDataFetcher(FieldDefinition fieldDef) {
            super(fieldDef, null)
        }

        @Override
        Object get(DataFetchingEnvironment environment) {
            if (!graphQLScalarTypes.containsKey(fieldDef.type)) {
                if ("true".equals(fieldDef.isList)) {
                    return new ArrayList<Object>()
                }
                return new HashMap<String, Object>()
            }
            return null
        }
    }

}
