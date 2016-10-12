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
import graphql.schema.GraphQLSchema
import graphql.schema.GraphQLType
import graphql.schema.GraphQLTypeReference
import graphql.schema.GraphQLUnionType
import graphql.schema.TypeResolver
import org.apache.commons.collections.map.HashedMap
import org.moqui.context.ArtifactAuthorizationException
import org.moqui.context.ArtifactTarpitException
import org.moqui.context.AuthenticationRequiredException
import org.moqui.context.ExecutionContext
import org.moqui.entity.EntityCondition
import org.moqui.entity.EntityFind
import org.moqui.entity.EntityValue
import org.moqui.impl.context.ExecutionContextFactoryImpl
import org.moqui.impl.context.ExecutionContextImpl
import org.moqui.impl.context.UserFacadeImpl
import org.moqui.impl.entity.EntityDefinition
import org.moqui.impl.entity.FieldInfo
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
    protected final ExecutionContextFactoryImpl ecfi
    @SuppressWarnings("GrFinalVariableAccess")
    protected final MNode schemaNode

    @SuppressWarnings("GrFinalVariableAccess")
    public final String schemaName
    @SuppressWarnings("GrFinalVariableAccess")
    protected final String queryType
    @SuppressWarnings("GrFinalVariableAccess")
    protected final String mutationType

    protected final Map<String, GraphQLType> graphQLTypeMap = new LinkedHashMap<>()
    protected final Set<String> graphQLTypeReferences = new HashSet<>()
    protected final Set<GraphQLType> inputTypes = new HashSet<GraphQLType>()

    protected final ArrayList<String> inputTypeList = new ArrayList<>()
    protected final List<String> preLoadObjectTypes = new LinkedList<>()

    protected Map<String, GraphQLTypeDefinition> allTypeDefMap = new LinkedHashMap<>()
    protected LinkedList<GraphQLTypeDefinition> allTypeDefSortedList = new LinkedList<>()
    protected Map<String, GraphQLTypeDefinition> requiredTypeDefMap = new LinkedHashMap<>()

//    protected Map<String, UnionTypeDefinition> unionTypeDefMap = new HashMap<>()
//    protected Map<String, EnumTypeDefinition> enumTypeDefMap = new HashMap<>()
    protected Map<String, InterfaceTypeDefinition> interfaceTypeDefMap = new LinkedHashMap<>()
//    protected Map<String, ObjectTypeDefinition> objectTypeDefMap = new HashMap<>()
    protected Map<String, ExtendObjectDefinition> extendObjectDefMap = new LinkedHashMap<>()

    protected GraphQLObjectType pageInfoType
    protected GraphQLInputObjectType paginationInputType
    protected GraphQLInputObjectType operationInputType
    protected GraphQLInputObjectType dateRangeInputType
    protected GraphQLArgument paginationArgument

    public static final Map<String, GraphQLArgument> directiveArgumentMap = new LinkedHashMap<>()

    static {
        directiveArgumentMap.put("if", GraphQLArgument.newArgument().name("if")
                                            .type(GraphQLBoolean)
                                            .description("Directive @if")
                                            .build())
    }


    public static final Map<String, GraphQLType> graphQLScalarTypes = [
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

    public GraphQLSchemaDefinition(ServiceFacade sf, MNode schemaNode) {
        this.sf = sf
        this.ecfi = ((ServiceFacadeImpl) sf).ecfi
        this.schemaNode = schemaNode

        this.schemaName = schemaNode.attribute("name")
        this.queryType = schemaNode.attribute("query")
        this.mutationType = schemaNode.attribute("mutation")

        createPredefinedGraphQLTypes()
        GraphQLSchemaUtil.createObjectTypeNodeForAllEntities(this.ecfi.getExecutionContext(), allTypeDefMap)

        for (MNode childNode in schemaNode.children) {
            switch (childNode.name) {
                case "input-type":
                    inputTypeList.add(childNode.attribute("name"))
                    break
                case "interface":
                    InterfaceTypeDefinition interfaceTypeDef = new InterfaceTypeDefinition(childNode, this.ecfi.getExecutionContext())
                    allTypeDefMap.put(childNode.attribute("name"), interfaceTypeDef)
                    interfaceTypeDefMap.put(childNode.attribute("name"), interfaceTypeDef)
                    break
                case "object":
                    allTypeDefMap.put(childNode.attribute("name"), new ObjectTypeDefinition(childNode, this.ecfi.getExecutionContext()))
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

    private GraphQLTypeDefinition getTypeDef(String name) {
        return allTypeDefMap.get(name)
//        return allTypeNodeList.find({ name.equals(it.name) })
    }

    private populateSortedTypes() {
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

        logger.info("==== allTypeNodeSortedList begin ====")
        for (GraphQLTypeDefinition typeDef in allTypeDefSortedList) {
            logger.info("[${typeDef.name} - ${typeDef.type}]")
        }
        logger.info("==== allTypeNodeSortedList end ====")
    }

    private void traverseByLevelOrder(TreeNode<GraphQLTypeDefinition> startNode, LinkedList<GraphQLTypeDefinition> sortedList) {
        Queue<TreeNode<GraphQLTypeDefinition>> queue = new LinkedList<>()
        queue.add(startNode)
        while(!queue.isEmpty()) {
            TreeNode<GraphQLTypeDefinition> tempNode = queue.poll()
            if (tempNode.data) {
                logger.info("Traversing node [${tempNode.data.name}]")
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
        logger.info("Post order traversing node [${startNode.data.name}]")
        if (!sortedList.contains(startNode.data)) {
            sortedList.add(startNode.data)
        }
    }

    private void createTreeNodeRecursive(TreeNode<GraphQLTypeDefinition> node, List<String> objectTypeNames, Boolean includeInterface) {
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
                    logger.info("Adding tree node for GraphQLTypeDefinition [${typeDef.name}]")
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
        populateSortedTypes()

        graphQLTypeMap.clear()
        addGraphQLPredefinedTypes()

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
        GraphQLType schemaQueryType = graphQLTypeMap.get(this.queryType)
        if (schemaQueryType == null)
            throw new IllegalArgumentException("GraphQL query type [${this.queryType}] for schema [${this.schemaName}] not found")

        GraphQLSchema.Builder schemaBuilder = GraphQLSchema.newSchema().query((GraphQLObjectType)schemaQueryType)

        if (this.mutationType) {
            GraphQLType schemaMutationType = graphQLTypeMap.get(this.mutationType)
            if (schemaQueryType == null)
                throw new IllegalArgumentException("GraphQL mutation type [${this.mutationType}] for schema [${this.schemaName}] not found")
            schemaBuilder = schemaBuilder.mutation((GraphQLObjectType) schemaMutationType)
        }

        inputTypes.clear()
        addSchemaInputTypes()

        logger.info("==== graphQLTypeMap begin ====")
        for (Map.Entry<String, GraphQLType> entry in graphQLTypeMap){
            logger.info(("GraphQLType [${entry.getKey()} - ${((GraphQLType) entry.getValue()).name} - ${entry.getValue().getClass()}]"))
        }
        logger.info("==== graphQLTypeMap end ====")

        GraphQLSchema schema = schemaBuilder.build(inputTypes)

        logger.info("Schema [${schemaName}] loaded: ${unionTypeCount} union type, ${enumTypeCount} enum type, ${interfaceTypeCount} interface type, ${objectTypeCount} object type")

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

            InterfaceTypeDefinition interfaceTypeDef = new InterfaceTypeDefinition(objectTypeDef, extendObjectDef, ecfi.getExecutionContext())
            allTypeDefMap.put(interfaceTypeDef.name, interfaceTypeDef)
            interfaceTypeDefMap.put(interfaceTypeDef.name, interfaceTypeDef)

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

    private void addSchemaInputTypes() {
        // Add default GraphQLScalarType
        for (Map.Entry<String, Object> entry in graphQLScalarTypes.entrySet()) {
            inputTypes.add((GraphQLType) entry.getValue())
        }

        inputTypes.add(paginationInputType)
        inputTypes.add(operationInputType)
        inputTypes.add(dateRangeInputType)

        // Add explicitly defined input types from *.graphql.xml
        for (String inputTypeName in inputTypeList) {
            GraphQLType type = graphQLTypeMap.get(inputTypeName)
            if (type == null)
                throw new IllegalArgumentException("GraphQL type [${inputTypeName}] for schema input types not found")
            inputTypes.add(type)
        }
    }

    private void addGraphQLPredefinedTypes() {
        // Add Scalar types
        for (String name in graphQLScalarTypes.keySet()) {
            graphQLTypeMap.put(name, graphQLScalarTypes.get(name))
        }

        graphQLTypeMap.put("GraphQLPageInfo", pageInfoType)
        graphQLTypeMap.put("PaginationInputType", paginationInputType)

        graphQLTypeMap.put("OperationInputType", operationInputType)
        graphQLTypeMap.put("DateRangeInputType", dateRangeInputType)
    }

    private void createPredefinedGraphQLTypes() {
        // This GraphQLPageInfo type is used for pagination
        // Pagination structure is
        // {
        //      data: [type],
        //      pageInfo: {     // GraphQLPageInfo type
        //          pageIndex, pageSize, totalCount, pageMaxIndex, pageRangeLow, pageRangeHigh
        //      }
        // }
        this.pageInfoType = GraphQLObjectType.newObject().name("GraphQLPageInfo")
                .field(createPredefinedField("pageIndex", GraphQLInt, ""))
                .field(createPredefinedField("pageSize", GraphQLInt, ""))
                .field(createPredefinedField("totalCount", GraphQLInt, ""))
                .field(createPredefinedField("pageMaxIndex", GraphQLInt, ""))
                .field(createPredefinedField("pageRangeLow", GraphQLInt, ""))
                .field(createPredefinedField("pageRangeHigh", GraphQLInt, ""))
                .build()

        this.paginationInputType = GraphQLInputObjectType.newInputObject().name("PaginationInputType")
                .field(createPredefinedInputField("type", GraphQLString, "PaginationInputType", ""))
                .field(createPredefinedInputField("pageIndex", GraphQLInt, 0, "Page index for pagination, default 0"))
                .field(createPredefinedInputField("pageSize", GraphQLInt, 20, "Page size for pagination, default 20"))
                .field(createPredefinedInputField("pageNoLimit", GraphQLBoolean, false, "Page no limit for pagination, default false"))
                .field(createPredefinedInputField("orderByField", GraphQLString, null, "OrderBy field for pagination. \ne.g. \n" +
                                     "productName \n" + "productName,statusId \n" + "-statusId,productName"))
                .build()

        this.operationInputType = GraphQLInputObjectType.newInputObject().name("OperationInputType")
                .field(createPredefinedInputField("type", GraphQLString, "OperationInputType", ""))
                .field(createPredefinedInputField("op", GraphQLString, null, "Operation on field, one of [ equals | like | contains | begins | empty | in ]"))
                .field(createPredefinedInputField("value", GraphQLString, null, "Argument value"))
                .field(createPredefinedInputField("not", GraphQLString, null, "Not operation, one of [ Y | true ] represents true"))
                .field(createPredefinedInputField("ic", GraphQLString, null, "Case insensitive, one of [ Y | true ] represents true"))
                .build()

        this.dateRangeInputType = GraphQLInputObjectType.newInputObject().name("DateRangeInputType")
                .field(createPredefinedInputField("type", GraphQLString, "DateRangeInputType", ""))
                .field(createPredefinedInputField("period", GraphQLChar, null, ""))
                .field(createPredefinedInputField("poffset", GraphQLChar, null, ""))
                .field(createPredefinedInputField("from", GraphQLChar, null, ""))
                .field(createPredefinedInputField("thru", GraphQLChar, null, ""))
                .build()

        this.paginationArgument = GraphQLArgument.newArgument().name("pagination")
                .type(paginationInputType)
                .description("pagination").build()
    }

    private GraphQLFieldDefinition createPredefinedField(String name, GraphQLOutputType type, String description) {
        GraphQLFieldDefinition.Builder fieldBuilder = GraphQLFieldDefinition.newFieldDefinition()
                .name(name).type(type).description(description)
        for (Map.Entry<String, GraphQLArgument> entry in directiveArgumentMap)
            fieldBuilder.argument(entry.getValue())
        return fieldBuilder.build()
    }

    static private GraphQLInputObjectField createPredefinedInputField(String name, GraphQLInputType type,
                                                              Object defaultValue, String description) {
        GraphQLInputObjectField.Builder fieldBuilder = GraphQLInputObjectField.newInputObjectField()
            .name(name).type(type).defaultValue(defaultValue).description(description)
        return fieldBuilder.build()
    }

    private void addGraphQLUnionType(UnionTypeDefinition unionTypeDef) {
        if (graphQLTypeMap.containsKey(unionTypeDef.name)) return

        GraphQLUnionType.Builder unionType = GraphQLUnionType.newUnionType()
                .name(unionTypeDef.name)
                .description(unionTypeDef.description)
        Map<String, GraphQLType> unionTypeList = new HashMap<>()

        for (String typeName in unionTypeDef.typeList) {
            GraphQLType type = graphQLTypeMap.get(typeName)
            if (type == null) {
                throw new IllegalArgumentException("GraphQL type [${typeName}] for union type [${unionTypeDef.name}] not found")
            } else if (!(type instanceof GraphQLObjectType)) {
                throw new ClassCastException("GraphQL type [${typeName}] for union type [${unionTypeDef.name}] is not GraphQLObjectType")
            }
            unionTypeList.put(typeName, type)
            unionType = unionType.possibleType(type)
        }

        // TODO: Add typeResolver for type, one way is to add a service as resolver

        graphQLTypeMap.put(unionTypeDef.name, unionType.build())
    }

    private void addGraphQLEnumType(EnumTypeDefinition enumTypeDef) {
        if (graphQLTypeMap.containsKey(enumTypeDef.name)) return

        GraphQLEnumType.Builder enumType = GraphQLEnumType.newEnum().name(enumTypeDef.name)
                .description(enumTypeDef.description)

        for (EnumValue valueNode in enumTypeDef.valueList) {
            enumType = enumType.value(valueNode.name, valueNode.value, valueNode.description, valueNode.depreciationReason)
        }

        graphQLTypeMap.put(enumTypeDef.name, enumType.build())
    }

    private void addGraphQLInterfaceType(InterfaceTypeDefinition interfaceTypeDef) {
        if (graphQLTypeMap.containsKey(interfaceTypeDef.name)) return

        GraphQLInterfaceType.Builder interfaceType = GraphQLInterfaceType.newInterface()
                .name(interfaceTypeDef.name)
                .description(interfaceTypeDef.description)

        for (FieldDefinition fieldNode in interfaceTypeDef.fieldList) {
            interfaceType.field(buildField(fieldNode))
        }

        // TODO: Add typeResolver for type, one way is to add a service as resolver
        if (!interfaceTypeDef.convertFromObjectTypeName.isEmpty()) {
            logger.info("~~~~~~~~~~~~~~~~ Interface typeResolver Adding")
            if (interfaceTypeDef.resolverField == null || interfaceTypeDef.resolverField.isEmpty())
                throw new IllegalArgumentException("Interface definition of ${interfaceTypeDef.name} resolverField not set")

            interfaceType.typeResolver(new TypeResolver() {
                @Override
                GraphQLObjectType getType(Object object) {

                    logger.info("~~~~~~~~~~~~~~~~ Interface typeResolver interfaceTypeDef ${interfaceTypeDef.name}")
                    logger.info("~~~~~~~~~~~~~~~~ Interface typeResolver interfaceTypeDef ${interfaceTypeDef.resolverField}")
                    logger.info("~~~~~~~~~~~~~~~~ Interface typeResolver interfaceTypeDef ${interfaceTypeDef.resolverMap}")
                    logger.info("~~~~~~~~~~~~~~~~ Interface typeResolver interfaceTypeDef ${interfaceTypeDef.defaultResolvedTypeName}")
                    logger.info("~~~~~~~~~~~~~~~~ Interface typeResolver getType ${object}")
                    logger.info("~~~~~~~~~~~~~~~~ Interface typeResolver getType ${object.getClass()}")
                    String resolverFieldValue = ((Map) object).get(interfaceTypeDef.resolverField)
                    String resolvedTypeName = interfaceTypeDef.resolverMap.get(resolverFieldValue)

                    logger.info("~~~~~~~~~~~~~~~~ Interface typeResolver getType ${resolverFieldValue}")
                    logger.info("~~~~~~~~~~~~~~~~ Interface typeResolver getType ${resolvedTypeName}")
                    GraphQLType resolvedType = graphQLTypeMap.get(resolvedTypeName)
                    if (resolvedType == null) resolvedType = graphQLTypeMap.get(interfaceTypeDef.defaultResolvedTypeName)
                    return (GraphQLObjectType) resolvedType
                }
            })
        }

        graphQLTypeMap.put(interfaceTypeDef.name, interfaceType.build())
    }

    private void addGraphQLObjectType(ObjectTypeDefinition objectTypeDef) {
        if (graphQLTypeMap.containsKey(objectTypeDef.name)) return

        GraphQLObjectType.Builder objectType = GraphQLObjectType.newObject()
                .name(objectTypeDef.name)
                .description(objectTypeDef.description)

        for (String interfaceName in objectTypeDef.interfaceList) {
            GraphQLType interfaceType = graphQLTypeMap.get(interfaceName)
            if (interfaceType == null)
                throw new IllegalArgumentException("GraphQL interface type ${interfaceName} for [${objectTypeDef.name}] not found.")

            objectType = objectType.withInterface((GraphQLInterfaceType) interfaceType)
            logger.info("==== addGraphQLObjectType ${objectTypeDef.name} interface ${interfaceType.name}")
        }

        for (FieldDefinition fieldDef in objectTypeDef.fieldList) {
            objectType = objectType.field(buildField(fieldDef))
        }

        graphQLTypeMap.put(objectTypeDef.name, objectType.build())
    }

    private GraphQLFieldDefinition buildField(FieldDefinition fieldDef) {
        GraphQLFieldDefinition.Builder graphQLFieldDef = GraphQLFieldDefinition.newFieldDefinition()
                .name(fieldDef.name)
                .description(fieldDef.description)
                .deprecate(fieldDef.depreciationReason)

        GraphQLType fieldRawType = graphQLTypeMap.get(fieldDef.type)
        if (fieldRawType == null) {
            logger.info("${fieldDef.name}")
            logger.info("${fieldDef.type}")
            fieldRawType = new GraphQLTypeReference(fieldDef.type)
            graphQLTypeMap.put(fieldDef.type, fieldRawType)
            graphQLTypeReferences.add(fieldDef.type)
        }

        // build type for field which could be one of: type, type!, [type], [type!], [type!]!
        GraphQLType fieldType
        if ("true".equals(fieldDef.isList)) {
            String listFieldTypeName = fieldDef.type + '__Pagination'
            fieldType = graphQLTypeMap.get(listFieldTypeName)
            if (fieldType == null) {
                // Create pagination object type for field.
                GraphQLType wrappedListFieldType
                if ("true".equals(fieldDef.listItemNonNull)) {
                    wrappedListFieldType = new GraphQLList(new GraphQLNonNull(fieldRawType))
                } else {
                    wrappedListFieldType = new GraphQLList(fieldRawType)
                }
                fieldType = GraphQLObjectType.newObject().name(listFieldTypeName)
                        .field(createPredefinedField("data", wrappedListFieldType, "Actual data list"))
                        .field(createPredefinedField("pageInfo", pageInfoType, "Pagination information"))
                        .build()
                graphQLTypeMap.put(listFieldTypeName, fieldType)
            }
        }
        if ("true".equals(fieldDef.nonNull)) {
            if (fieldType == null) {
                fieldType = new GraphQLNonNull(fieldRawType)
            } else {
                fieldType = new GraphQLNonNull(fieldType)
            }
        }
        if (fieldType == null) fieldType = fieldRawType

        if (!(fieldType instanceof GraphQLOutputType))
            throw new IllegalArgumentException("GraphQL type [${fieldDef.type}] for field [${fieldDef.name}] is not derived from GraphQLOutputType")

        graphQLFieldDef.type((GraphQLOutputType) fieldType)

        // build arguments for field
        for (ArgumentDefinition argNode in fieldDef.argumentList)
            graphQLFieldDef.argument(buildArgument(argNode))
        // Add pagination argument
        if ("true".equals(fieldDef.isList)) graphQLFieldDef.argument(paginationArgument)
        // Add directive arguments
        for (Map.Entry<String, GraphQLArgument> entry in directiveArgumentMap)
            graphQLFieldDef.argument((GraphQLArgument) entry.getValue())

        if (fieldDef.dataFetcher != null) {
            graphQLFieldDef.dataFetcher(new DataFetcher() {
                @Override
                public Object get(DataFetchingEnvironment environment) {
                    return fieldDef.dataFetcher.get(environment)
                }
            })
        }

        return graphQLFieldDef.build()
    }

    private GraphQLArgument buildArgument(ArgumentDefinition argumentDef) {
        GraphQLArgument.Builder argument = GraphQLArgument.newArgument()
                .name(argumentDef.name)
                .description(argumentDef.description)
                .defaultValue(argumentDef.defaultValue)

        GraphQLType argType = graphQLTypeMap.get(argumentDef.type)
        if (argType == null)
            throw new IllegalArgumentException("GraphQL type [${argumentDef.type}] for argument [${argumentDef.name}] not found")

        if (!(argType instanceof GraphQLInputType))
            throw new IllegalArgumentException("GraphQL type [${argumentDef.type}] for argument [${argumentDef.name}] is not derived from GraphQLInputObjectType")

        if ("true".equals(argumentDef.fieldDef.isList)) {
            if (graphQLDateTypes.contains(argumentDef.type)) {
                argType = graphQLTypeMap.get("DateRangeInputType")
            } else if (graphQLStringTypes.contains(argumentDef.type) || graphQLNumericTypes.contains(argumentDef.type)) {
                argType = graphQLTypeMap.get("OperationInputType")
            }
        }

        argument = argument.type((GraphQLInputType) argType)

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
            this.interfaceList = interfaceList
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
                // Ignore already defined field in object type, behave like field in object type override interface type
                if (fieldDefMap.containsKey(entry.getKey())) continue
                fieldDefMap.put(entry.getKey(), ((FieldDefinition) entry.getValue()).clone())
            }
            interfaceTypeDefinition.addResolver(interfaceNode.attribute("resolver-value"), interfaceNode.attribute("resolver-type"))
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

        Boolean convertToInterface = false

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
        FieldDefinition fieldDef

        ArgumentDefinition(MNode node, FieldDefinition fieldDef) {
            this.fieldDef = fieldDef
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
            this.fieldDef = fieldDef
            this.name = name
            this.attributeMap.putAll(attributeMap)
        }

        ArgumentDefinition(FieldDefinition fieldDef, String name, String type, String required, String defaultValue, String description) {
            this.fieldDef = fieldDef
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

        public void setFieldDef(FieldDefinition fieldDef) {
            this.fieldDef = fieldDef
        }

        @Override
        public ArgumentDefinition clone() {
            return new ArgumentDefinition(this.fieldDef, this.name, this.attributeMap)
        }
    }

    static class FieldDefinition implements Cloneable {
        ExecutionContext ec
        String name, type, description, depreciationReason
        String nonNull, isList, listItemNonNull
        String requireAuthentication

        DataFetcherHandler dataFetcher
        String preDataFetcher, postDataFetcher

        List<ArgumentDefinition> argumentList = new LinkedList<>()

        FieldDefinition(MNode node, ExecutionContext ec) {
            this.ec = ec
            this.name = node.attribute("name")
            this.type = node.attribute("type")
            this.description = node.attribute("description")
            this.nonNull = node.attribute("non-null") ?: "false"
            this.isList = node.attribute("is-list") ?: "false"
            this.listItemNonNull = node.attribute("list-item-non-null") ?: "false"
            this.requireAuthentication = node.attribute("require-authentication") ?: "true"

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
                        mergeArgument(new ArgumentDefinition(childNode, this))
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
            updateFieldDefOnArgumentDefs()
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
                        DataFetcherHandler dataFetcher, List<String> excludedFields) {
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

            addAutoArguments(excludedFields)
            updateFieldDefOnArgumentDefs()
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

        private void updateFieldDefOnArgumentDefs() {
            for (ArgumentDefinition argumentNode in argumentList)
                argumentNode.setFieldDef(this)
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
            ArgumentDefinition baseArgumentDef = argumentList.find({ it.name == argumentName })
            if (baseArgumentDef == null) {
                baseArgumentDef = new ArgumentDefinition(this, argumentName, attributeMap)
                argumentList.add(baseArgumentDef)
            } else {
                baseArgumentDef.attributeMap.putAll(attributeMap)
            }
            return baseArgumentDef
        }

        private void addAutoArguments(List<String> excludedFields) {
            if (graphQLScalarTypes.keySet().contains(type) || directiveArgumentMap.keySet().contains(type)) return
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
                ArgumentDefinition argumentDef = new ArgumentDefinition(this, fi.name,
                        GraphQLSchemaUtil.fieldTypeGraphQLMap.get(fi.type), null, null, fieldDescription)
                argumentList.add(argumentDef)
            }
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
                Map result = ec.getService().sync().name(serviceName)
                        .parameter("environment", environment)
                        .parameters(ec.context).call()

                return result
            } finally {
                if (loggedInAnonymous) ((UserFacadeImpl) ec.getUser()).logoutAnonymousOnly()
            }
        }
    }

    static class DataFetcherEntity extends DataFetcherHandler{
        String entityName, interfaceEntityName, operation
        String requireAuthentication
        String interfaceEntityPkField
        Map<String, String> relKeyMap = new HashMap<>()

        DataFetcherEntity(MNode node, FieldDefinition fieldDef, ExecutionContext ec) {
            super(fieldDef, ec)
            this.requireAuthentication = fieldDef.requireAuthentication ?: "true"
            this.entityName = node.attribute("entity-name")
            this.interfaceEntityName = node.attribute("interface-entity-name")

            if ("true".equals(fieldDef.isList)) this.operation = "list"
            else this.operation = "one"

            if (interfaceEntityName) {
                EntityDefinition ed = ((ExecutionContextImpl) ec).getEntityFacade().getEntityDefinition(interfaceEntityName)
                ec.logger.info("getting interface entity ${interfaceEntityName} definition...")
                ec.logger.info("${ed.entityName}")
                ec.logger.info("${ed.getFieldNames(true, false)}")
                if (ed.getFieldNames(true, false).size() != 1)
                    throw new IllegalArgumentException("Entity ${interfaceEntityName} for interface should have one primary key")
                interfaceEntityPkField = ed.getFieldNames(true, false).first()
            }
        }

        DataFetcherEntity(ExecutionContext ec, FieldDefinition fieldDef, String entityName, Map<String, String> relKeyMap) {
            super(fieldDef, ec)
            this.requireAuthentication = fieldDef.requireAuthentication ?: "true"
            this.entityName = entityName
            this.relKeyMap.putAll(relKeyMap)
            if ("true".equals(fieldDef.isList)) { this.operation = "list" }
            else { this.operation = "one" }
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
                transformArguments(environment.arguments, inputFieldsMap)
                if (operation == "one") {
                    EntityFind ef = ec.entity.find(entityName).searchFormMap(inputFieldsMap, null, null, false)
                    for (Map.Entry<String, String> entry in relKeyMap.entrySet()) {
                        ef = ef.condition(entry.getValue(), ((Map) environment.source).get(entry.getKey()))
                    }
                    EntityValue one = ef.one()
                    if (one == null) return  null
                    if (interfaceEntityName == null || interfaceEntityName.isEmpty()) {
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
                    EntityFind ef = ec.entity.find(entityName).searchFormMap(inputFieldsMap, null, null, false)
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

                    Map<String, Object> resultMap = new HashMap<>()
                    resultMap.put("pageInfo", ['pageIndex': pageIndex, 'pageSize': pageSize, 'totalCount': count,
                           'pageMaxIndex': pageMaxIndex, 'pageRangeLow': pageRangeLow, 'pageRangeHigh': pageRangeHigh]) as Map<String, Object>

                    List<Map<String, Object>> list = ef.list().getPlainValueList(0)
                    if (list == null || list.size() == 0) {
                        resultMap.put("data", null)
                    } else {
                        if (interfaceEntityName == null || interfaceEntityName.isEmpty()) {
                            resultMap.put("data", list)
                        } else {
                            List<Object> pkValues = new ArrayList<>()
                            for (Map<String, Object> one in list) pkValues.add(one.get(interfaceEntityPkField))
                            ef = ec.entity.find(interfaceEntityName).condition(interfaceEntityPkField, EntityCondition.ComparisonOperator.IN, pkValues)
                            List<Map<String, Object>> interfaceValueList = ef.list().getPlainValueList(0)
                            List<Map<String, Object>> jointOneList = new ArrayList<>(list.size())
                            Map<String, Object> jointOneMap, matchedOne
                            for (Map<String, Object> interfaceValue in interfaceValueList) {
                                jointOneMap = new HashedMap()
                                jointOneMap.putAll(interfaceValue)
                                matchedOne = list.find({ interfaceValue.get(interfaceEntityPkField).equals(it.get(interfaceEntityPkField)) })
                                if (matchedOne != null) jointOneMap.putAll(matchedOne)
                                jointOneList.add(jointOneMap)
                            }
                            resultMap.put("data", jointOneList)
                        }
                    }
                    return resultMap
                }
            }
            finally {
                if (loggedInAnonymous) ((UserFacadeImpl) ec.getUser()).logoutAnonymousOnly()
            }
            return null
        }

        static void transformArguments(Map<String, Object> arguments, Map<String, Object> inputFieldsMap) {
            for (Map.Entry<String, Object> entry in arguments.entrySet()) {
                String argName = entry.getKey()
                // Ignore if argument which is used for directive @include and @skip
                if ("if".equals(argName)) continue
                Object argValue = entry.getValue()
                if (argValue == null) continue
                logger.info("------- argument ${argName} value [${argValue}]")
                logger.info("------- argument ${argName} class [${argValue.getClass()}]")
                logger.info("------- argument ${argName} instanceof Map [${argValue instanceof Map}]")
                logger.info("------- argument ${argName} instanceof LinkedHashMap [${argValue instanceof LinkedHashMap}]")

                if (argValue instanceof LinkedHashMap) {
                    // currently the defaultValue on GraphQLInputObjectField does not work
                    /*
                    if ("OperationInputType".equals(argValue.get("type"))) {
                        logger.info("------- == checking OperationInputType variable ${argName} to inputFieldsMap with value ${argValue}")
                        if (argValue.get("value") != null ) inputFieldsMap.put(argName, argValue.get("value"))
                        if (argValue.get("op") != null) inputFieldsMap.put(argName + "_op", argValue.get("op"))
                        if (argValue.get("not") != null) inputFieldsMap.put(argName + "_not", argValue.get("not"))
                        if (argValue.get("ic") != null) inputFieldsMap.put(argName + "_ic", argValue.get("ic"))
                    } else if ("DateRangeInputType".equals(argValue.get("type"))) {
                        // Add _period, _offset, _from, _thru
                        for (Map.Entry<String, Object> argEntry in argValue.entrySet()) {
                            if (argEntry.getValue() == null || "type".equals(argEntry.getKey())) continue
                            inputFieldsMap.put(argName + "_" + argEntry.getKey(), argEntry.getValue())
                        }
                    } else if ("PaginationInputType".equals(argValue.get("type"))) {
                        // Add pageIndex, pageSize, pageNoLimit, orderByField
                        for (Map.Entry<String, Object> argEntry in argValue.entrySet()) {
                            if (argEntry.getValue() == null || "type".equals(argEntry.getKey())) continue
                            logger.info("------- == adding pagination variable ${argEntry.getKey()} to inputFieldsMap with value ${argEntry.getValue()}")
                            inputFieldsMap.put(argEntry.getKey(), argEntry.getValue())
                        }
                    }
                    */

                    if (argValue.get("value") != null) inputFieldsMap.put(argName, argValue.get("value"))
                    if (argValue.get("op") != null) inputFieldsMap.put(argName + "_op", argValue.get("op"))
                    if (argValue.get("not") != null) inputFieldsMap.put(argName + "_not", argValue.get("not"))
                    if (argValue.get("ic") != null) inputFieldsMap.put(argName + "_ic", argValue.get("ic"))
                    inputFieldsMap.put("pageIndex", argValue.get("pageIndex") ?: 0)
                    inputFieldsMap.put("pageSize", argValue.get("pageSize") ?: 20)
                    if (argValue.get("pageNoLimit") != null) inputFieldsMap.put("pageNoLimit", argValue.get("pageNoLimit"))
                    if (argValue.get("orderByField") != null) inputFieldsMap.put("orderByField", argValue.get("orderByField"))

                    if (argValue.get("period") != null) inputFieldsMap.put(argName + "_period", argValue.get("period"))
                    if (argValue.get("poffset") != null) inputFieldsMap.put(argName + "_poffset", argValue.get("poffset"))
                    if (argValue.get("from") != null) inputFieldsMap.put(argName + "_from", argValue.get("from"))
                    if (argValue.get("thru") != null) inputFieldsMap.put(argName + "_thru", argValue.get("thru"))

                } else {
                    inputFieldsMap.put(argName, argValue)
                }
            }
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
