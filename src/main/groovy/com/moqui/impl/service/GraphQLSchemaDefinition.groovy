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
import org.moqui.context.ExecutionContext
import org.moqui.entity.EntityFind
import org.moqui.impl.context.ExecutionContextFactoryImpl
import org.moqui.impl.context.ExecutionContextImpl
import org.moqui.impl.context.UserFacadeImpl
import org.moqui.impl.entity.EntityDefinition
import org.moqui.impl.service.ServiceFacadeImpl
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

    protected Map<String, GraphQLTypeDefinition> allTypeNodeMap = new HashMap<>()
    protected LinkedList<GraphQLTypeDefinition> allTypeNodeSortedList = new LinkedList<>()

//    protected Map<String, UnionTypeDefinition> unionTypeNodeMap = new HashMap<>()
//    protected Map<String, EnumTypeDefinition> enumTypeNodeMap = new HashMap<>()
    protected Map<String, InterfaceTypeDefinition> interfaceTypeNodeMap = new HashMap<>()
//    protected Map<String, ObjectTypeDefinition> objectTypeNodeMap = new HashMap<>()
    protected Map<String, ExtendObjectDefinition> extendObjectNodeMap = new HashMap<>()

    protected GraphQLObjectType pageInfoType
    protected GraphQLInputObjectType paginationInputType
    protected GraphQLInputObjectType operationInputType
    protected GraphQLInputObjectType dateRangeInputType

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

        createGraphQLPredefinedTypes()
        GraphQLSchemaUtil.createObjectTypeNodeForAllEntities(this.ecfi.getExecutionContext(), allTypeNodeMap)

        for (MNode childNode in schemaNode.children) {
            switch (childNode.name) {
                case "input-type":
                    inputTypeList.add(childNode.attribute("name"))
                    break
                case "interface":
                    allTypeNodeMap.put(childNode.attribute("name"), new InterfaceTypeDefinition(childNode, this.ecfi.getExecutionContext()))
                    break
                case "object":
                    allTypeNodeMap.put(childNode.attribute("name"), new ObjectTypeDefinition(childNode, this.ecfi.getExecutionContext()))
                    break
                case "union":
                    allTypeNodeMap.put(childNode.attribute("name"), new UnionTypeDefinition(childNode))
                    break
                case "enum":
                    allTypeNodeMap.put(childNode.attribute("name"), new EnumTypeDefinition(childNode))
                    break
                case "extend-object":
                    extendObjectNodeMap.put(childNode.attribute("name"), new ExtendObjectDefinition(childNode))
                    break
            }
        }

        updateAllTypeNodeMap()
    }

    private GraphQLTypeDefinition getTypeNode(String name) {
        return allTypeNodeMap.get(name)
//        return allTypeNodeList.find({ name.equals(it.name) })
    }

    private populateSortedTypes() {
        allTypeNodeSortedList.clear()

        GraphQLTypeDefinition queryTypeNode = getTypeNode(queryType)
        GraphQLTypeDefinition mutationTypeNode = getTypeNode(mutationType)

        if (queryTypeNode == null) {
            logger.error("No query type [${queryType}] defined for GraphQL schema")
            return
        }

        allTypeNodeSortedList.push(queryTypeNode)
        if (mutationType) allTypeNodeSortedList.push(mutationTypeNode)

        TreeNode<GraphQLTypeDefinition> rootNode = new TreeNode<>(null)
        rootNode.children.add(new TreeNode<GraphQLTypeDefinition>(queryTypeNode))
        if (mutationTypeNode) {
            rootNode.children.add(new TreeNode<GraphQLTypeDefinition>(mutationTypeNode))
        }


        createTreeNodeRecursive(rootNode, [queryType, mutationType])
        traverseByLevelOrder(rootNode)

        logger.info("==== allTypeNodeSortedList ====")
        for (GraphQLTypeDefinition node in allTypeNodeSortedList) {
            logger.info("[${node.name} - ${node.type}]")
        }
    }

    private void traverseByLevelOrder(TreeNode<GraphQLTypeDefinition> startNode) {
        Queue<TreeNode<GraphQLTypeDefinition>> queue = new LinkedList<>()
        queue.add(startNode)
        while(!queue.isEmpty()) {
            TreeNode<GraphQLTypeDefinition> tempNode = queue.poll()
            if (tempNode.data) {
                logger.info("Traversing node [${tempNode.data.name}]")
                if (!allTypeNodeSortedList.contains(tempNode.data)) {
                    allTypeNodeSortedList.addFirst(tempNode.data)
                }
            }
            for (TreeNode<GraphQLTypeDefinition> childNode in tempNode.children) {
                queue.add(childNode)
            }
        }
    }

    private void createTreeNodeRecursive(TreeNode<GraphQLTypeDefinition> node, List<String> objectTypeNames) {
        if (node.data) {
            for (String type in node.data.getDependentTypes()) {
                // If type is GraphQL Scalar types, skip.
                if (graphQLScalarTypes.containsKey(type)) continue
                // If type is GraphQLObjectType which already added in Tree, skip.
                if (objectTypeNames.contains(type)) continue

                GraphQLTypeDefinition typeNode = getTypeNode(type)
                if (typeNode != null) {
                    TreeNode<GraphQLTypeDefinition> typeTreeNode = new TreeNode<>(typeNode)
                    node.children.add(typeTreeNode)
                    objectTypeNames.push(type)
                    logger.info("Adding tree node for GraphQLTypeDefinition [${typeNode.name}]")
                    createTreeNodeRecursive(typeTreeNode, objectTypeNames)
                } else {
                    logger.error("No GraphQL Type [${type}] defined")
                }
            }
        } else {
            for (TreeNode<GraphQLTypeDefinition> childTreeNode in node.children) {
                createTreeNodeRecursive(childTreeNode, objectTypeNames)
            }
        }
    }

    public GraphQLSchema getSchema() {
        populateSortedTypes()

        graphQLTypeMap.clear()
        addGraphQLPredefinedTypes()

        for (GraphQLTypeDefinition typeNode in allTypeNodeSortedList) {
            switch (typeNode.type) {
                case "union":
                    addGraphQLUnionType((UnionTypeDefinition) typeNode)
                    break
                case "enum":
                    addGraphQLEnumType((EnumTypeDefinition) typeNode)
                    break
                case "interface":
                    addGraphQLInterfaceType((InterfaceTypeDefinition) typeNode)
                    break
                case "object":
                    addGraphQLObjectType((ObjectTypeDefinition) typeNode)
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

        GraphQLSchema schema = schemaBuilder.build(inputTypes)

        // Clear out reference to unused GraphQLType, used ones are still referenced by schema
        graphQLTypeMap.clear()

        return schema
    }

    private void updateAllTypeNodeMap() {
        for (Map.Entry<String, ExtendObjectDefinition> entry in extendObjectNodeMap) {
            ObjectTypeDefinition objectTypeNode = allTypeNodeMap.get(entry.getKey())
            if (objectTypeNode == null) {
                logger.info("ObjectTypeDefinition [${entry.getKey()}] not found to extend")
                continue
            }
            ExtendObjectDefinition extendObjectNode = (ExtendObjectDefinition) entry.getValue()
            if ("true".equals(extendObjectNode.asInterface)) {

            }

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

    private void createGraphQLPredefinedTypes() {
        // This GraphQLPageInfo type is used for pagination
        // Pagination structure is
        // {
        //      data: [type],
        //      pageInfo: {     // GraphQLPageInfo type
        //          pageIndex, pageSize, totalCount, pageMaxIndex, pageRangeLow, pageRangeHigh
        //      }
        // }
        this.pageInfoType = GraphQLObjectType.newObject().name("GraphQLPageInfo")
                .field(GraphQLFieldDefinition.newFieldDefinition()
                        .name("pageIndex").type(GraphQLInt).build())
                .field(GraphQLFieldDefinition.newFieldDefinition()
                        .name("pageSize").type(GraphQLInt).build())
                .field(GraphQLFieldDefinition.newFieldDefinition()
                        .name("totalCount").type(GraphQLInt).build())
                .field(GraphQLFieldDefinition.newFieldDefinition()
                        .name("pageMaxIndex").type(GraphQLInt).build())
                .field(GraphQLFieldDefinition.newFieldDefinition()
                        .name("pageRangeLow").type(GraphQLInt).build())
                .field(GraphQLFieldDefinition.newFieldDefinition()
                        .name("pageRangeHigh").type(GraphQLInt).build())
                .build()

        this.paginationInputType = GraphQLInputObjectType.newInputObject().name("PaginationInputType")
                .field(GraphQLInputObjectField.newInputObjectField()
                        .name("type").type(GraphQLString).defaultValue("PaginationInputType").build())
                .field(GraphQLInputObjectField.newInputObjectField()
                        .name("pageIndex").type(GraphQLInt).defaultValue(0)
                        .description("Page index for pagination, default 0").build())
                .field(GraphQLInputObjectField.newInputObjectField()
                        .name("pageSize").type(GraphQLInt).defaultValue(20)
                        .description("Page size for pagination, default 20").build())
                .field(GraphQLInputObjectField.newInputObjectField()
                        .name("pageNoLimit").type(GraphQLBoolean).defaultValue(false)
                        .description("Page no limit for pagination, default false").build())
                .field(GraphQLInputObjectField.newInputObjectField()
                        .name("orderByField").type(GraphQLString)
                        .description("OrderBy field for pagination. \ne.g. \n" +
                                     "productName \n" + "productName,statusId \n" + "-statusId,productName")
                        .build())
                .build()

        this.operationInputType = GraphQLInputObjectType.newInputObject().name("OperationInputType")
                .field(GraphQLInputObjectField.newInputObjectField()
                        .name("type").type(GraphQLString).defaultValue("OperationInputType").build())
                .field(GraphQLInputObjectField.newInputObjectField().name("op").type(GraphQLString)
                        .description("Operation on field, one of [ equals | like | contains | begins | empty | in ]")
                        .build())
                .field(GraphQLInputObjectField.newInputObjectField().name("value").type(GraphQLString)
                        .description("Argument value").build())
                .field(GraphQLInputObjectField.newInputObjectField().name("not").type(GraphQLString)
                        .description("Not operation, one of [ Y | true ] represents true").build())
                .field(GraphQLInputObjectField.newInputObjectField().name("ic").type(GraphQLString)
                        .description("Case insensitive, one of [ Y | true ] represents true").build())
                .build()

        this.dateRangeInputType = GraphQLInputObjectType.newInputObject().name("DateRangeInputType")
                .field(GraphQLInputObjectField.newInputObjectField()
                        .name("type").type(GraphQLString).defaultValue("DateRangeInputType").build())
                .field(GraphQLInputObjectField.newInputObjectField().name("period").type(GraphQLChar)
                        .description("").build())
                .field(GraphQLInputObjectField.newInputObjectField().name("poffset").type(GraphQLChar)
                        .description("").build())
                .field(GraphQLInputObjectField.newInputObjectField().name("from").type(GraphQLChar)
                        .description("").build())
                .field(GraphQLInputObjectField.newInputObjectField().name("thru").type(GraphQLChar)
                        .description("").build())
                .build()

    }

    private void addGraphQLUnionType(UnionTypeDefinition node) {
        GraphQLUnionType.Builder unionType = GraphQLUnionType.newUnionType()
                .name(node.name)
                .description(node.description)
        Map<String, GraphQLType> unionTypeList = new HashMap<>()

        for (String typeName in node.typeList) {
            GraphQLType type = graphQLTypeMap.get(typeName)
            if (type == null) {
                throw new IllegalArgumentException("GraphQL type [${typeName}] for union type [${node.name}] not found")
            } else if (!(type instanceof GraphQLObjectType)) {
                throw new ClassCastException("GraphQL type [${typeName}] for union type [${node.name}] is not GraphQLObjectType")
            }
            unionTypeList.put(typeName, type)
            unionType = unionType.possibleType(type)
        }

        // TODO: Add typeResolver for type, one way is to add a service as resolver

        graphQLTypeMap.put(node.name, unionType.build())
    }

    private void addGraphQLEnumType(EnumTypeDefinition node) {
        GraphQLEnumType.Builder enumType = GraphQLEnumType.newEnum().name(node.name)
                .description(node.description)

        for (EnumValue valueNode in node.valueList) {
            enumType = enumType.value(valueNode.name, valueNode.value, valueNode.description, valueNode.depreciationReason)
        }

        graphQLTypeMap.put(node.name, enumType.build())
    }

    private void addGraphQLInterfaceType(InterfaceTypeDefinition node) {
        GraphQLInterfaceType.Builder interfaceType = GraphQLInterfaceType.newInterface()
                .name(node.name)
                .description(node.description)

        for (FieldDefinition fieldNode in node.fieldList) {
            interfaceType = interfaceType.field(buildField(fieldNode))
        }

        // TODO: Add typeResolver for type, one way is to add a service as resolver

        graphQLTypeMap.put(node.name, interfaceType.build())
    }

    private void addGraphQLObjectType(ObjectTypeDefinition node) {
        GraphQLObjectType.Builder objectType = GraphQLObjectType.newObject()
                .name(node.name)
                .description(node.description)

        for (String interfaceName in node.interfaceList) {
            GraphQLType interfaceType = graphQLTypeMap.get(interfaceName)
            if (interfaceType == null)
                throw new IllegalArgumentException("GraphQL interface type [${node.name}] not found.")

            objectType = objectType.withInterface((GraphQLInterfaceType) interfaceType)
        }

        for (FieldDefinition fieldNode in node.fieldList) {
            objectType = objectType.field(buildField(fieldNode))
        }

        graphQLTypeMap.put(node.name, objectType.build())
    }

    private GraphQLFieldDefinition buildField(FieldDefinition fieldNode) {
        GraphQLFieldDefinition.Builder fieldDef = GraphQLFieldDefinition.newFieldDefinition()
                .name(fieldNode.name)
                .description(fieldNode.description)
                .deprecate(fieldNode.depreciationReason)

        GraphQLType fieldRawType = graphQLTypeMap.get(fieldNode.type)
        if (fieldRawType == null) {
            fieldRawType = new GraphQLTypeReference(fieldNode.type)
            graphQLTypeMap.put(fieldNode.name, fieldRawType)
            graphQLTypeReferences.add(fieldNode.type)
        }

        // build type for field which could be one of: type, type!, [type], [type!], [type!]!
        GraphQLType fieldType
        if ("true".equals(fieldNode.isList)) {
            String listFieldTypeName = fieldNode.type + '__Pagination'
            fieldType = graphQLTypeMap.get(listFieldTypeName)
            if (fieldType == null) {
                // Create pagination object type for field.
                GraphQLType wrappedListFieldType
                if ("true".equals(fieldNode.listItemNonNull)) {
                    wrappedListFieldType = new GraphQLList(new GraphQLNonNull(fieldRawType))
                } else {
                    wrappedListFieldType = new GraphQLList(fieldRawType)
                }
                fieldType = GraphQLObjectType.newObject().name(listFieldTypeName)
                        .field(GraphQLFieldDefinition.newFieldDefinition().name("data")
                                .type(wrappedListFieldType).build())
                        .field(GraphQLFieldDefinition.newFieldDefinition().name("pageInfo")
                                .type(pageInfoType).build())
                        .build()
                graphQLTypeMap.put(listFieldTypeName, fieldType)
            }
        }
        if ("true".equals(fieldNode.nonNull)) {
            if (fieldType == null) {
                fieldType = new GraphQLNonNull(fieldRawType)
            } else {
                fieldType = new GraphQLNonNull(fieldType)
            }
        }
        if (fieldType == null) fieldType = fieldRawType

        if (!(fieldType instanceof GraphQLOutputType))
            throw new IllegalArgumentException("GraphQL type [${fieldNode.type}] for field [${fieldNode.name}] is not derived from GraphQLOutputType")

        fieldDef = fieldDef.type((GraphQLOutputType) fieldType)

        // build arguments for field
        for (ArgumentDefinition argNode in fieldNode.argumentList) {
            fieldDef.argument(buildArgument(argNode))
        }

        if (fieldNode.dataFetcher != null) {
            fieldDef.dataFetcher(new DataFetcher() {
                @Override
                public Object get(DataFetchingEnvironment environment) {
                    return fieldNode.dataFetcher.get(environment)
                }
            })
        }

        return fieldDef.build()
    }

    private GraphQLArgument buildArgument(ArgumentDefinition node) {
        GraphQLArgument.Builder argument = GraphQLArgument.newArgument()
                .name(node.name)
                .description(node.description)
                .defaultValue(node.defaultValue)

        GraphQLType argType = graphQLTypeMap.get(node.type)
        if (argType == null)
            throw new IllegalArgumentException("GraphQL type [${node.type}] for argument [${node.name}] not found")

        if (!(argType instanceof GraphQLInputType))
            throw new IllegalArgumentException("GraphQL type [${node.type}] for argument [${node.name}] is not derived from GraphQLInputObjectType")

        if ("true".equals(node.fieldNode.isList)) {
            if (graphQLDateTypes.contains(node.type)) {
                argType = graphQLTypeMap.get("DateRangeInputType")
            } else if (graphQLStringTypes.contains(node.type) || graphQLNumericTypes.contains(node.type)) {
                argType = graphQLTypeMap.get("OperationInputType")
            }
        }

        argument = argument.type((GraphQLInputType) argType)

        return argument.build()
    }

    public static class TreeNode<T> {
        T data
        public final List<TreeNode<T>> children = new ArrayList<TreeNode<T>>()

        public TreeNode(data) { this.data = data }
    }

    static abstract class GraphQLTypeDefinition {
        String name, description, type

        ArrayList<String> getDependentTypes() { }
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
        ArrayList<EnumValue> valueList = new ArrayList<>()

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
        ArrayList<String> getDependentTypes() {
            return new ArrayList<String>()
        }
    }

    static  class UnionTypeDefinition extends GraphQLTypeDefinition {
        String typeResolver
        ArrayList<String> typeList = new LinkedList<>()

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
        ArrayList<String> getDependentTypes() {
            return typeList
        }
    }


    static class InterfaceTypeDefinition extends GraphQLTypeDefinition {
        @SuppressWarnings("GrFinalVariableAccess")
        final ExecutionContext ec

        Boolean convertFromObjectType

        String typeResolver
        Map<String, FieldDefinition> fieldNodeMap = new HashMap<>()
        String resolverField
        Map<String, String> resolverMap = new HashMap<>()

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
                        fieldNodeMap.put(childNode.attribute("name"), new FieldDefinition(childNode, ec))
//                        typeList.add(childNode.attribute("type"))
                        break
                }
            }
        }

        InterfaceTypeDefinition(ObjectTypeDefinition objectTypeNode, ExtendObjectDefinition extendObjectNode, ExecutionContext ec) {
            convertFromObjectType = true
            fieldNodeMap.putAll(objectTypeNode.fieldNodeMap)

            for (FieldDefinition fieldNode in extendObjectNode.fieldNodeMap.values()) {
                mergeField(fieldNode)
            }
        }

        public ArrayList<FieldDefinition> getFieldList() { return new ArrayList<FieldDefinition>(fieldNodeMap.values()) }

        @Override
        ArrayList<String> getDependentTypes() { return new ArrayList<String>(fieldNodeMap.values().type) }

        private void mergeField(FieldDefinition fieldNode) {

        }
    }

    static class ObjectTypeDefinition extends GraphQLTypeDefinition {
        @SuppressWarnings("GrFinalVariableAccess")
        final ExecutionContext ec

        ArrayList<String> interfaceList = new ArrayList<>()
        Map<String, InterfaceTypeDefinition> interfacesMap
        Map<String, FieldDefinition> fieldNodeMap = new HashMap<>()

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
                        fieldNodeMap.put(childNode.attribute("name"), new FieldDefinition(childNode, ec))
//                        typeList.add(childNode.attribute("type"))
                        break
                }
            }
        }

        ObjectTypeDefinition(ExecutionContext ec, String name, String description, List<String> interfaceList,
                             Map<String, FieldDefinition> fieldNodeMap) {
            this.ec = ec
            this.name = name
            this.description = description
            this.type = "object"
            this.interfaceList = interfaceList
            this.fieldNodeMap.putAll(fieldNodeMap)
        }

        public List<FieldDefinition> getFieldList() { return new ArrayList<FieldDefinition>(fieldNodeMap.values()) }

        @Override
        ArrayList<String> getDependentTypes() { return new ArrayList<String>(fieldNodeMap.values().type) }
    }

    static class ExtendObjectDefinition {
        @SuppressWarnings("GrFinalVariableAccess")
        final ExecutionContext ec
        String name, asInterface, resolverField

        ArrayList<String> interfaceList = new ArrayList<>()
        Map<String, FieldDefinition> fieldNodeMap = new HashMap<>()
        Map<String, String> resolverMap = new HashMap<>()

        ExtendObjectDefinition(MNode node) {
            this.name = node.attribute("name")
            this.asInterface = node.attribute("as-interface") ?: "false"
            this.resolverField = node.attribute("resolver-field")

            for (MNode childNode in node.children) {
                switch (childNode.name) {
                    case "interface":
                        interfaceList.add(childNode.attribute("name"))
                        break
                    case "field":
                        fieldNodeMap.put(childNode.attribute("name"), new FieldDefinition(childNode, ec))
                        break
                    case "resolver-map":
                        if (resolverField != null && !resolverField.isEmpty()) {
                            resolverMap.put(childNode.attribute("resolver-value"), childNode.attribute("resolver-type"))
                        }
                        break
                }
            }
        }
    }

    static class AutoArgumentsDefinition {
        String entityName, include, required
        List<String> excludes = new ArrayList<>()

        AutoArgumentsDefinition(MNode node) {
            this.entityName = node.attribute("entity-name")
            this.include = node.attribute("include") ?: "all"
            this.required = node.attribute("required") ?: "false"
            for (MNode childNode in node.children("exclude")) {
                excludes.add(childNode.attribute("field-name"))
            }
        }
    }

    static class ArgumentDefinition {
        String name
        Map<String, String> attributeMap = new HashMap<>()
        FieldDefinition fieldNode

        ArgumentDefinition(MNode node, FieldDefinition fieldNode) {
            this.fieldNode = fieldNode
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

        ArgumentDefinition(FieldDefinition fieldNode, String name, Map<String, String> attributeMap) {
            this.fieldNode = fieldNode
            this.name = name
            this.attributeMap.putAll(attributeMap)
        }

        ArgumentDefinition(String name, String type, String required, String defaultValue, String description) {
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

        public void setFieldNode(FieldDefinition fieldNode) {
            this.fieldNode = fieldNode
        }
    }

    static class FieldDefinition {
        ExecutionContext ec
        String name, type, description, depreciationReason
        String nonNull, isList, listItemNonNull
        String requireAuthentication

        DataFetcherHandler dataFetcher
        String preDataFetcher, postDataFetcher

        List<ArgumentDefinition> argumentList = new ArrayList<>()
        List<FieldDefinition> fieldList = new ArrayList<>()

        FieldDefinition(MNode node, ExecutionContext ec) {
            this.ec = ec
            this.name = node.attribute("name")
            this.type = node.attribute("type")
            this.description = node.attribute("description")
            this.nonNull = node.attribute("non-null")
            this.isList = node.attribute("is-list")
            this.listItemNonNull = node.attribute("list-item-non-null")
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
                    case "field":
                        this.fieldList.add(new FieldDefinition(childNode, ec))
                        break
                }
            }

            updateFieldNodeOnArgumentNodes()
        }

        FieldDefinition(ExecutionContext ec, String name, String type) {
            this(ec, name, type, new HashMap<>(), null, new ArrayList<>(), new ArrayList<>())
        }

        FieldDefinition(ExecutionContext ec, String name, String type, Map<String, String> fieldPropertyMap) {
            this(ec, name, type, fieldPropertyMap, null, new ArrayList<>(), new ArrayList<>())
        }

        FieldDefinition(ExecutionContext ec, String name, String type, Map<String, String> fieldPropertyMap,
                        List<ArgumentDefinition> argumentList) {
            this(ec, name, type, fieldPropertyMap, null, argumentList, new ArrayList<>())
        }

        FieldDefinition(ExecutionContext ec, String name, String type, Map<String, String> fieldPropertyMap,
                        DataFetcherHandler dataFetcher, List<ArgumentDefinition> argumentList, List<FieldDefinition> fieldList) {
            this.ec = ec
            this.name = name
            this.type = type
            this.dataFetcher = dataFetcher
            this.argumentList.addAll(argumentList)
            this.fieldList.addAll(fieldList)

            this.nonNull = fieldPropertyMap.get("nonNull")
            this.isList = fieldPropertyMap.get("isList")
            this.listItemNonNull = fieldPropertyMap.get("listItemNonNull")
            this.requireAuthentication = fieldPropertyMap.get("requireAuthentication") ?: "true"

            this.description = fieldPropertyMap.get("description")
            this.depreciationReason = fieldPropertyMap.get("depreciationReason")

            updateFieldNodeOnArgumentNodes()
        }

        private void updateFieldNodeOnArgumentNodes() {
            for (ArgumentDefinition argumentNode in argumentList)
                argumentNode.setFieldNode(this)
        }

        public void setDataFetcher(DataFetcherHandler dataFetcher) {
            this.dataFetcher = dataFetcher
        }

        private void mergeArgument(ArgumentDefinition argumentNode) {
            mergeArgument(argumentNode.name, argumentNode.attributeMap)
        }

        private void mergeArgument(AutoArgumentsDefinition autoArgumentsNode) {
            String entityName = autoArgumentsNode.entityName
            if (entityName == null || entityName.isEmpty())
                throw new IllegalArgumentException("Error in auto-arguments in field ${this.name}, no auto-arguments.@entity-name")
            ExecutionContextImpl eci = (ExecutionContextImpl) ec
            EntityDefinition ed = eci.getEntityFacade().getEntityDefinition(entityName)
            if (ed == null) throw new IllegalArgumentException("Error in auto-arguments in field ${this.name}, the entity-name is not a valid entity name")

            String includeStr = autoArgumentsNode.include
            for (String fieldName in ed.getFieldNames("all".equals(includeStr) || "pk".equals(includeStr), "all".equals(includeStr) || "nonpk".equals(includeStr))) {
                if (autoArgumentsNode.excludes.contains(fieldName)) continue

                Map<String, String> map = new HashMap<>(4)
                map.put("type", GraphQLSchemaUtil.getEntityFieldGraphQLType(ed.getFieldInfo(fieldName).type))
                map.put("required", autoArgumentsNode.required)
                map.put("defaultValue", "")
                map.put("description", "")
                mergeArgument(fieldName, map)
            }
        }

        private ArgumentDefinition mergeArgument(final String argumentName, Map<String, String> attributeMap) {
            ArgumentDefinition baseArgumentNode = argumentList.find({ it.name == argumentName })
            if (baseArgumentNode == null) {
                baseArgumentNode = new ArgumentDefinition(this, argumentName, attributeMap)
                argumentList.add(baseArgumentNode)
            } else {
                baseArgumentNode.attributeMap.putAll(attributeMap)
            }
            return baseArgumentNode
        }
    }

    static abstract class DataFetcherHandler {
        @SuppressWarnings("GrFinalVariableAccess")
        final ExecutionContext ec
        @SuppressWarnings("GrFinalVariableAccess")
        final FieldDefinition fieldNode

        DataFetcherHandler(FieldDefinition fieldNode, ExecutionContext ec) {
            this.ec = ec
            this.fieldNode = fieldNode
        }

        Object get(DataFetchingEnvironment environment) { return null }
    }

    static class DataFetcherService extends DataFetcherHandler {
        String serviceName
        String requireAuthentication

        DataFetcherService(MNode node, FieldDefinition fieldNode, ExecutionContext ec) {
            super(fieldNode, ec)
            this.requireAuthentication = node.attribute("require-authentication") ?: fieldNode.requireAuthentication ?: "true"

            this.serviceName = node.attribute("service")
        }

        @Override
        Object get(DataFetchingEnvironment environment) {
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
        String entityName, operation
        String requireAuthentication
        Map<String, String> relKeyMap = new HashMap<>()

        DataFetcherEntity(MNode node, FieldDefinition fieldNode, ExecutionContext ec) {
            super(fieldNode, ec)
            this.requireAuthentication = fieldNode.requireAuthentication ?: "true"
            this.entityName = node.attribute("entity-name")
            this.operation = node.attribute("operation")
        }

        DataFetcherEntity(ExecutionContext ec, FieldDefinition fieldNode, String entityName, String operation, Map<String, String> relKeyMap) {
            super(fieldNode, ec)
            this.requireAuthentication = fieldNode.requireAuthentication ?: "true"
            this.entityName = entityName
            this.operation = operation
            this.relKeyMap.putAll(relKeyMap)
        }

        @Override
        Object get(DataFetchingEnvironment environment) {
            logger.info("---- running data fetcher entity for entity [${entityName}] with operation [${operation}] ...")
            logger.info("arguments  - ${environment.arguments}")
            logger.info("source     - ${environment.source}")
            logger.info("context    - ${environment.context}")
            logger.info("relKeyMap  - ${relKeyMap}")

            boolean loggedInAnonymous = false
            if ("anonymous-all".equals(requireAuthentication)) {
                ec.artifactExecution.setAnonymousAuthorizedAll()
                loggedInAnonymous = ec.getUser().loginAnonymousIfNoUser()
            } else if ("anonymous-view".equals(requireAuthentication)) {
                ec.artifactExecution.setAnonymousAuthorizedView()
                loggedInAnonymous = ec.getUser().loginAnonymousIfNoUser()
            }

            try {
                if (operation == "one") {
                    putArgsIntoContext(environment.arguments, ec)
                    EntityFind ef = ec.entity.find(entityName).searchFormMap(ec.context, null, null, false)
                    for (Map.Entry<String, String> entry in relKeyMap.entrySet()) {
                        ef = ef.condition(entry.getValue(), ((Map) environment.source).get(entry.getKey()))
                    }
                    return ef.one().getMap()
                } else if (operation == "list") {
                    putArgsIntoContext(environment.arguments, ec)

                    EntityFind ef = ec.entity.find(entityName).searchFormMap(ec.context, null, null, false)
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
                    resultMap.put("data", ef.list().getPlainValueList(0))
                    resultMap.put("pageInfo", ['pageIndex': pageIndex, 'pageSize': pageSize, 'totalCount': count,
                           'pageMaxIndex': pageMaxIndex, 'pageRangeLow': pageRangeLow, 'pageRangeHigh': pageRangeHigh]) as Map<String, Object>

                    return resultMap
                }
            } finally {
                if (loggedInAnonymous) ((UserFacadeImpl) ec.getUser()).logoutAnonymousOnly()
            }
        }

        void putArgsIntoContext(Map<String, Object> arguments, ExecutionContext ec) {
            for (Map.Entry<String, Object> entry in arguments.entrySet()) {
                String argName = entry.getKey()
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
                        logger.info("------- == checking OperationInputType variable ${argName} to ec.context with value ${argValue}")
                        if (argValue.get("value") != null ) ec.context.put(argName, argValue.get("value"))
                        if (argValue.get("op") != null) ec.context.put(argName + "_op", argValue.get("op"))
                        if (argValue.get("not") != null) ec.context.put(argName + "_not", argValue.get("not"))
                        if (argValue.get("ic") != null) ec.context.put(argName + "_ic", argValue.get("ic"))
                    } else if ("DateRangeInputType".equals(argValue.get("type"))) {
                        // Add _period, _offset, _from, _thru
                        for (Map.Entry<String, Object> argEntry in argValue.entrySet()) {
                            if (argEntry.getValue() == null || "type".equals(argEntry.getKey())) continue
                            ec.context.put(argName + "_" + argEntry.getKey(), argEntry.getValue())
                        }
                    } else if ("PaginationInputType".equals(argValue.get("type"))) {
                        // Add pageIndex, pageSize, pageNoLimit, orderByField
                        for (Map.Entry<String, Object> argEntry in argValue.entrySet()) {
                            if (argEntry.getValue() == null || "type".equals(argEntry.getKey())) continue
                            logger.info("------- == adding pagination variable ${argEntry.getKey()} to ec.context with value ${argEntry.getValue()}")
                            ec.context.put(argEntry.getKey(), argEntry.getValue())
                        }
                    }
                    */

                    if (argValue.get("value") != null) ec.context.put(argName, argValue.get("value"))
                    if (argValue.get("op") != null) ec.context.put(argName + "_op", argValue.get("op"))
                    if (argValue.get("not") != null) ec.context.put(argName + "_not", argValue.get("not"))
                    if (argValue.get("ic") != null) ec.context.put(argName + "_ic", argValue.get("ic"))
                    ec.context.put("pageIndex", argValue.get("pageIndex") ?: 0)
                    ec.context.put("pageSize", argValue.get("pageSize") ?: 20)
                    if (argValue.get("pageNoLimit") != null) ec.context.put("pageNoLimit", argValue.get("pageNoLimit"))
                    if (argValue.get("orderByField") != null) ec.context.put("orderByField", argValue.get("orderByField"))

                    if (argValue.get("period") != null) ec.context.put(argName + "_period", argValue.get("period"))
                    if (argValue.get("poffset") != null) ec.context.put(argName + "_poffset", argValue.get("poffset"))
                    if (argValue.get("from") != null) ec.context.put(argName + "_from", argValue.get("from"))
                    if (argValue.get("thru") != null) ec.context.put(argName + "_thru", argValue.get("thru"))

                } else {
                    ec.context.put(argName, argValue)
                }
            }
        }
    }

    static class EmptyDataFetcher extends DataFetcherHandler {
        EmptyDataFetcher (MNode node, FieldDefinition fieldNode) {
            super(fieldNode, null)
        }

        @Override
        Object get(DataFetchingEnvironment environment) {
            if (!graphQLScalarTypes.containsKey(fieldNode.type)) {
                if ("true".equals(fieldNode.isList)) {
                    return new ArrayList<Object>()
                }
                return new HashMap<String, Object>()
            }
            return null
        }
    }

}
