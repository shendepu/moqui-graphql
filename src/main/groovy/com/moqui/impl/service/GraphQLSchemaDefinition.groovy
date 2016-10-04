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

import graphql.Scalars
import graphql.schema.DataFetcher
import graphql.schema.DataFetchingEnvironment
import graphql.schema.GraphQLArgument
import graphql.schema.GraphQLEnumType
import graphql.schema.GraphQLFieldDefinition
import graphql.schema.GraphQLInputObjectType
import graphql.schema.GraphQLInterfaceType
import graphql.schema.GraphQLList
import graphql.schema.GraphQLNonNull
import graphql.schema.GraphQLObjectType
import graphql.schema.GraphQLOutputType
import graphql.schema.GraphQLSchema
import graphql.schema.GraphQLType
import graphql.schema.GraphQLTypeReference
import graphql.schema.GraphQLUnionType
import org.moqui.impl.context.ExecutionContextFactoryImpl
import org.moqui.impl.service.ServiceFacadeImpl
import org.moqui.service.ServiceFacade
import org.moqui.util.MNode
import org.slf4j.Logger
import org.slf4j.LoggerFactory

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
    protected final Map<String, GraphQLType> graphQLTypeReferenceMap = new LinkedHashMap<>()
    protected final Set<GraphQLType> inputTypes = new HashSet<GraphQLType>()

    protected final ArrayList<String> inputTypeList = new ArrayList<>()

    protected ArrayList<GraphQLTypeNode> allTypeNodeList = new ArrayList<>()
    protected LinkedList<GraphQLTypeNode> allTypeNodeSortedList = new LinkedList<>()

    protected ArrayList<UnionTypeNode> unionTypeNodeList = new ArrayList<>()
    protected ArrayList<EnumTypeNode> enumTypeNodeList = new ArrayList<>()
    protected ArrayList<InterfaceTypeNode> interfaceTypeNodeList = new ArrayList<>()
    protected ArrayList<ObjectTypeNode> objectTypeNodeList = new ArrayList<>()

    public static final Map<String, GraphQLType> graphQLScalarTypes = [
            "Int"       : Scalars.GraphQLInt, "Long": Scalars.GraphQLLong,
            "Float"     : Scalars.GraphQLFloat, "String": Scalars.GraphQLString,
            "Boolean"   : Scalars.GraphQLBoolean, "ID": Scalars.GraphQLID,
            "BigInteger": Scalars.GraphQLBigInteger, "BigDecimal": Scalars.GraphQLBigDecimal,
            "Byte"      : Scalars.GraphQLByte, "Short": Scalars.GraphQLShort,
            "Char"      : Scalars.GraphQLChar]

    public GraphQLSchemaDefinition(ServiceFacade sf, MNode schemaNode) {
        this.sf = sf
        this.ecfi = ((ServiceFacadeImpl) sf).ecfi
        this.schemaNode = schemaNode

        this.schemaName = schemaNode.attribute("name")
        this.queryType = schemaNode.attribute("query")
        this.mutationType = schemaNode.attribute("mutation")

        for (MNode childNode in schemaNode.children) {
            switch (childNode.name) {
                case "input-type":
                    inputTypeList.add(childNode.attribute("name"))
                    break
                case "interface-type":
                    allTypeNodeList.add(new InterfaceTypeNode(childNode))
                    break
                case "object-type":
                    allTypeNodeList.add(new ObjectTypeNode(childNode))
                    break
                case "union-type":
                    allTypeNodeList.add(new UnionTypeNode(childNode))
                    break
                case "enum-type":
                    allTypeNodeList.add(new EnumTypeNode(childNode))
                    break
            }
        }
    }

    private GraphQLTypeNode getTypeNode(String name) {
        return allTypeNodeList.find({ it.name == name })
    }

    private populateSortedTypes() {
        allTypeNodeSortedList.clear()

        GraphQLTypeNode queryTypeNode = getTypeNode(queryType)
        GraphQLTypeNode mutationTypeNode = getTypeNode(mutationType)

        if (queryTypeNode == null) {
            logger.error("No query type [${queryType}] defined for GraphQL schema")
            return
        }

        allTypeNodeSortedList.push(queryTypeNode)
        if (mutationType) allTypeNodeSortedList.push(mutationTypeNode)

        TreeNode<GraphQLTypeNode> rootNode = new TreeNode<>(null)
        rootNode.children.add(new TreeNode<GraphQLTypeNode>(queryTypeNode))
        if (mutationTypeNode) {
            rootNode.children.add(new TreeNode<GraphQLTypeNode>(mutationTypeNode))
        }
        createTreeNodeRecursive(rootNode)
        traverseByLevelOrder(rootNode)

        logger.info("==== allTypeNodeSortedList ====")
        for (GraphQLTypeNode node in allTypeNodeSortedList) {
            logger.info("[${node.name} - ${node.type}]")
        }
    }

    private void traverseByLevelOrder(TreeNode<GraphQLTypeNode> startNode) {
        Queue<TreeNode<GraphQLTypeNode>> queue = new LinkedList<>()
        queue.add(startNode)
        while(!queue.isEmpty()) {
            TreeNode<GraphQLTypeNode> tempNode = queue.poll()
            if (tempNode.data) {
                logger.info("Traversing node [${tempNode.data.name}]")
                if (!allTypeNodeSortedList.contains(tempNode.data)) {
                    allTypeNodeSortedList.addFirst(tempNode.data)
                }
            }
            for (TreeNode<GraphQLTypeNode> childNode in tempNode.children) {
                queue.add(childNode)
            }
        }
    }

    private void createTreeNodeRecursive(TreeNode<GraphQLTypeNode> node) {
        if (node.data) {
            for (String type in node.data.getDependentTypes()) {
                // If type is GraphQL Scalar types, skip.
                if (graphQLScalarTypes.containsKey(type)) continue

                GraphQLTypeNode typeNode = getTypeNode(type)
                if (typeNode != null) {
                    TreeNode<GraphQLTypeNode> typeTreeNode = new TreeNode<>(typeNode)
                    node.children.add(typeTreeNode)
                    createTreeNodeRecursive(typeTreeNode)
                } else {
                    logger.error("No GraphQL Type [${type}] defined")
                }
            }
        } else {
            for (TreeNode<GraphQLTypeNode> childTreeNode in node.children) {
                createTreeNodeRecursive(childTreeNode)
            }
        }
    }

    public GraphQLSchema getSchema() {
        populateSortedTypes()

        graphQLTypeMap.clear()
        addGraphQLScalarTypes()


        for (GraphQLTypeNode typeNode in allTypeNodeSortedList) {
            switch (typeNode.type) {
                case "union":
                    addGraphQLUnionType((UnionTypeNode) typeNode)
                    break
                case "enum":
                    addGraphQLEnumType((EnumTypeNode) typeNode)
                    break
                case "interface":
                    addGraphQLInterfaceType((InterfaceTypeNode) typeNode)
                    break
                case "object":
                    addGraphQLObjectType((ObjectTypeNode) typeNode)
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

        for (String referenceTypeName in graphQLTypeReferenceMap.keySet()) {
            GraphQLType type = schema.allTypesAsList.find({ it.name = referenceTypeName})
            if (type != null) {
                logger.info("Replacing GraphQLTypeReference [${referenceTypeName}]")
                graphQLTypeMap.put(referenceTypeName, type)
            } else {
                logger.error("GraphQLTypeReference for [${referenceTypeName}] not found in schema")
            }
        }

        return schema
    }

    private void addSchemaInputTypes() {
        // Add default GraphQLScalarType
        for (GraphQLType scalarType in graphQLScalarTypes.values()) {
            inputTypes.add(scalarType)
        }

        // Add explicitly defined input types
        for (String inputTypeName in inputTypeList) {
            GraphQLType type = graphQLTypeMap.get(inputTypeName)
            if (type == null)
                throw new IllegalArgumentException("GraphQL type [${inputTypeName}] for schema input types not found")
            inputTypes.add(type)
        }
    }

    private void addGraphQLScalarTypes() {
        for (String name in graphQLScalarTypes.keySet()) {
            graphQLTypeMap.put(name, graphQLScalarTypes.get(name))
        }
    }

    private void addGraphQLUnionType(UnionTypeNode node) {
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

    private void addGraphQLEnumType(EnumTypeNode node) {
        GraphQLEnumType.Builder enumType = GraphQLEnumType.newEnum().name(node.name)
                .description(node.description)

        for (EnumValueNode valueNode in node.valueList) {
            enumType = enumType.value(valueNode.name, valueNode.value, valueNode.description, valueNode.depreciationReason)
        }

        graphQLTypeMap.put(node.name, enumType.build())
    }

    private void addGraphQLInterfaceType(InterfaceTypeNode node) {
        GraphQLInterfaceType.Builder interfaceType = GraphQLInterfaceType.newInterface()
                .name(node.name)
                .description(node.description)

        for (FieldNode fieldNode in node.fieldList) {
            interfaceType = interfaceType.field(buildField(fieldNode))
        }

        // TODO: Add typeResolver for type, one way is to add a service as resolver

        graphQLTypeMap.put(node.name, interfaceType.build())
    }

    private void addGraphQLObjectType(ObjectTypeNode node) {
        GraphQLObjectType.Builder objectType = GraphQLObjectType.newObject()
                .name(node.name)
                .description(node.description)

        for (String interfaceName in node.interfaceList) {
            GraphQLType interfaceType = graphQLTypeMap.get(interfaceName)
            if (interfaceType == null)
                throw new IllegalArgumentException("GraphQL interface type [${node.name}] not found.")

            objectType = objectType.withInterface((GraphQLInterfaceType) interfaceType)
        }

        for (FieldNode fieldNode in node.fieldList) {
            objectType = objectType.field(buildField(fieldNode))
        }

        graphQLTypeMap.put(node.name, objectType.build())
    }

    private GraphQLFieldDefinition buildField(FieldNode node) {
        GraphQLFieldDefinition.Builder fieldDef = GraphQLFieldDefinition.newFieldDefinition()
                .name(node.name)
                .description(node.description)
                .deprecate(node.depreciationReason)

        GraphQLType fieldRawType = graphQLTypeMap.get(node.type)
        if (fieldRawType == null ) {
            fieldRawType = new GraphQLTypeReference(node.type)
            graphQLTypeMap.put(node.name, fieldRawType)
            graphQLTypeReferenceMap.put(node.name, fieldRawType)
        }

        // build type for field which could be one of: type, type!, [type], [type!], [type!]!
        GraphQLType fieldType
        if (node.isList == "true") {
            if (node.listItemNonNull == "true") {
                fieldType = new GraphQLList(new GraphQLNonNull(fieldRawType))
            } else {
                fieldType = new GraphQLList(fieldRawType)
            }
        }
        if (node.nonNull == "true") {
            if (fieldType == null) {
                fieldType = new GraphQLNonNull(fieldRawType)
            } else {
                fieldType = new GraphQLNonNull(fieldType)
            }
        }
        if (fieldType == null) fieldType = fieldRawType

        if (!(fieldType instanceof GraphQLOutputType))
            throw new IllegalArgumentException("GraphQL type [${node.type}] for field [${node.name}] is not derived from GraphQLOutputType")

        fieldDef = fieldDef.type((GraphQLOutputType) fieldType)

        // build arguments for field
        for (ArgumentNode argNode in node.argumentList) {
            fieldDef.argument(buildArgument(argNode))
        }

        if (node.dataFetcher != null) {
            fieldDef.dataFetcher(new DataFetcher() {
                @Override
                public Object get(DataFetchingEnvironment environment) {
                    return node.dataFetcher.get(environment)
                }
            })
        }

        return fieldDef.build()
    }

    private GraphQLArgument buildArgument(ArgumentNode node) {
        GraphQLArgument.Builder argument = GraphQLArgument.newArgument()
                .name(node.name)
                .description(node.description)
                .defaultValue(node.defaultValue)

        GraphQLType argType = graphQLTypeMap.get(node.type)
        if (argType == null)
            throw new IllegalArgumentException("GraphQL type [${node.type}] for argument [${node.name}] not found")

        if (!(argType instanceof GraphQLInputObjectType))
            throw new IllegalArgumentException("GraphQL type [${node.type}] for argument [${node.name}] is not derived from GraphQLInputObjectType")

        argument = argument.type((GraphQLInputObjectType) argType)

        return argument.build()
    }

    public static class TreeNode<T> {
        T data
        public final List<TreeNode<T>> children = new ArrayList<TreeNode<T>>()

        public TreeNode(data) { this.data = data }
    }

    static abstract class GraphQLTypeNode {
        String name, description, type

        ArrayList<String> getDependentTypes() { }
    }

    static class EnumValueNode {
        String name, value, description, depreciationReason

        EnumValueNode(MNode node) {
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

    static class EnumTypeNode extends GraphQLTypeNode {
        ArrayList<EnumValueNode> valueList = new ArrayList<>()

        EnumTypeNode(MNode node) {
            this.name = node.attribute("name")
            this.type = "enum"

            for (MNode childNode in node.children) {
                switch (childNode.name) {
                    case "description":
                        this.description = childNode.text
                        break
                    case "enum-value":
                        valueList.add(new EnumValueNode(childNode))
                        break
                }
            }
        }

        @Override
        ArrayList<String> getDependentTypes() {
            return new ArrayList<String>()
        }
    }

    static  class UnionTypeNode extends GraphQLTypeNode {
        String typeResolver
        ArrayList<String> typeList = new LinkedList<>()

        UnionTypeNode(MNode node) {
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


    static class InterfaceTypeNode extends GraphQLTypeNode {
        String typeResolver
        ArrayList<FieldNode> fieldList = new ArrayList<>()
        ArrayList<String> typeList = new ArrayList<>()

        InterfaceTypeNode(MNode node) {
            this.name = node.attribute("name")
            this.type = "interface"
            this.typeResolver = node.attribute("type-resolver")
            for (MNode childNode in node.children) {
                switch (childNode.name) {
                    case "description":
                        this.description = childNode.text
                        break
                    case "field":
                        fieldList.add(new FieldNode(childNode))
                        typeList.add(childNode.attribute("type"))
                        break
                }
            }
        }

        @Override
        ArrayList<String> getDependentTypes() {
            return typeList
        }
    }

    static class ObjectTypeNode extends GraphQLTypeNode {
        String typeResolver
        ArrayList<String> interfaceList = new ArrayList<>()
        ArrayList<FieldNode> fieldList = new ArrayList<>()
        ArrayList<String> typeList = new ArrayList<>()

        ObjectTypeNode(MNode node) {
            this.name = node.attribute("name")
            this.type = "object"
            this.typeResolver = node.attribute("type-resolver")
            for (MNode childNode in node.children) {
                switch (childNode.name) {
                    case "description":
                        this.description = childNode.text
                        break
                    case "interface":
                        interfaceList.add(childNode.attribute("name"))
                        break
                    case "field":
                        fieldList.add(new FieldNode(childNode))
                        typeList.add(childNode.attribute("type"))
                        break
                }
            }
        }

        @Override
        ArrayList<String> getDependentTypes() {
            return typeList
        }
    }

    static class ArgumentNode {
        String name, type, defaultValue, description

        ArgumentNode(MNode node) {
            this.name = node.attribute("name")
            this.type = node.attribute("type")
            this.defaultValue = node.attribute("default-value")

            for (MNode childNode in node.children) {
                if (childNode.name == "description") {
                    this.description = node.attribute("description")
                }
            }
        }
    }

    static class FieldNode {
        String name, type, description, depreciationReason
        String nonNull, isList, listItemNonNull

        DataFetcherHandler dataFetcher

        List<ArgumentNode> argumentList = new ArrayList<>()
        List<FieldNode> fieldList = new ArrayList<>()

        FieldNode(MNode node) {
            this.name = node.attribute("name")
            this.type = node.attribute("type")
            this.description = node.attribute("description")
            this.nonNull = node.attribute("non-null")
            this.isList = node.attribute("is-list")
            this.listItemNonNull = node.attribute("list-item-non-null")

            for (MNode childNode in node.children) {
                switch (childNode.name) {
                    case "description":
                        this.description = childNode.text
                        break
                    case "depreciation-reason":
                        this.depreciationReason = childNode.text
                        break
                    case "argument":
                        this.argumentList.add(new ArgumentNode(childNode))
                        break
                    case "data-fetcher":
                        this.dataFetcher = new DataFetcherService(childNode)
                        break
                    case "field":
                        this.fieldList.add(new FieldNode(childNode))
                        break
                }
            }
        }
    }

    static abstract class DataFetcherHandler {
        Object get(DataFetchingEnvironment environment) { }
    }

    static class DataFetcherService extends DataFetcherHandler {
        String service
        String preDataFetcher, postDataFetcher

        DataFetcherService(MNode node) {
            ArrayList<MNode> preDataFetcherNodes = node.children("pre-data-fetcher")
            if (preDataFetcherNodes.size() > 0) {
                this.preDataFetcher = preDataFetcherNodes[0].attribute("service")
            }

            ArrayList<MNode> postDataFetcherNodes = node.children("post-data-fetcher")
            if (preDataFetcherNodes.size() > 0) {
                this.postDataFetcher = postDataFetcherNodes[0].attribute("service")
            }

            MNode serviceNode = node.children("service")[0]
            this.service = serviceNode.attribute("service")
        }
    }

}
