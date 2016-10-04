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

import graphql.schema.GraphQLSchema
import graphql.schema.GraphQLType
import groovy.transform.CompileStatic
import org.moqui.impl.context.ExecutionContextFactoryImpl
import org.moqui.impl.service.ServiceFacadeImpl
import org.moqui.jcache.MCache
import org.moqui.service.ServiceFacade
import org.moqui.util.MNode
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.xml.ws.Service

public class GraphQLSchemaDefinition {
    protected final static Logger logger = LoggerFactory.getLogger(GraphQLSchemaDefinition.class)

    @SuppressWarnings("GrFinalVariableAccess")
    public final ServiceFacade sf
    @SuppressWarnings("GrFinalVariableAccess")
    public final ExecutionContextFactoryImpl ecfi
    @SuppressWarnings("GrFinalVariableAccess")
    public final MNode schemaNode

    @SuppressWarnings("GrFinalVariableAccess")
    public final String schemaName
    @SuppressWarnings("GrFinalVariableAccess")
    public final String queryType
    @SuppressWarnings("GrFinalVariableAccess")
    public final String mutationType

    public final ArrayList<String> inputTypeList = new ArrayList<>()

    public ArrayList<GraphQLTypeNode> allTypeNodeList = new ArrayList<>()
    public LinkedList<GraphQLTypeNode> allTypeNodeSortedList = new LinkedList<>()

    public ArrayList<Map<String, UnionTypeNode>> unionTypeNodeList = new ArrayList<>()
    public ArrayList<Map<String, EnumTypeNode>> enumTypeNodeList = new ArrayList<>()
    public ArrayList<Map<String, TypeNode>> interfaceTypeNodeList = new ArrayList<>()
    public ArrayList<Map<String, TypeNode>> objectTypeNodeList = new ArrayList<>()

    public GraphQLSchemaDefinition(ServiceFacade sf, MNode schemaNode) {
        this.sf = sf
        this.ecfi = ((ServiceFacadeImpl) sf).ecfi
        this.schemaNode = schemaNode

        this.schemaName = schemaNode.attribute("name")
        this.queryType = schemaNode.attribute("queryType")
        this.mutationType = schemaNode.attribute("mutationType")

        for (MNode childNode in schemaNode.children) {
            switch (childNode.name) {
                case "input-type":
                    inputTypeList.add(childNode.attribute("name"))
                    break
                case "type":
                    allTypeNodeList.add(new TypeNode(childNode))
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

    public GraphQLTypeNode getTypeNode(String name) {
        return allTypeNodeList.find({ it.name == name })
    }

    public populateSortedTypes() {
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
        createTreeNodeRecusive(rootNode)
        traverseByLevelOrder(rootNode)

    }

    void traverseByLevelOrder(TreeNode<GraphQLTypeNode> startNode) {
        Queue<TreeNode<GraphQLTypeNode>> queue = new LinkedList<>()
        queue.add(startNode)
        while(!queue.isEmpty()) {
            TreeNode<GraphQLTypeNode> tempNode = queue.poll()
            if (tempNode.data) {
                logger.info("Traversing node [${tempNode.data.name}]")
                if (!allTypeNodeSortedList.contains(tempNode.data)) {
                    allTypeNodeSortedList.push(tempNode.data)
                }
            }
            for (TreeNode<GraphQLTypeNode> childNode in tempNode.children) {
                queue.add(childNode)
            }
        }
    }

    void createTreeNodeRecusive(TreeNode<GraphQLTypeNode> node) {
        if (node.data) {
            for (String type in node.data.getDependentTypes()) {
                GraphQLTypeNode typeNode = getTypeNode(type)
                if (typeNode != null) {
                    TreeNode<GraphQLTypeNode> typeTreeNode = new TreeNode<>(typeNode)
                    node.children.add(typeTreeNode)
                    createTreeNodeRecusive(typeTreeNode)
                } else {
                    logger.error("No GraphQL Type [${type}] defined")
                }
            }
        } else {
            for (TreeNode<GraphQLTypeNode> childTreeNode in node.children) {
                createTreeNodeRecusive(childTreeNode)
            }
        }
    }

    GraphQLSchema getSchema() {
        Map<String, GraphQLType> graphQLTypeMap = new HashMap<>()


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
        ArrayList<String> typeList = new ArrayList<>()

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

    static class TypeNode extends GraphQLTypeNode {
        String typeResolver
        ArrayList<FieldNode> fieldList = new ArrayList<>()
        ArrayList<String> typeList = new ArrayList<>()

        TypeNode(MNode node) {
            this.name = node.attribute("name")
            this.type = node.attribute("type")
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
        String nonNull, isList

        String preDataFetcher, postDataFetcher
        DataFetcherHandler dataFetcher

        List<ArgumentNode> argumentList = new ArrayList<>()
        List<FieldNode> fieldList = new ArrayList<>()

        FieldNode(MNode node) {
            this.name = node.attribute("name")
            this.type = node.attribute("type")
            this.description = node.attribute("description")
            this.nonNull = node.attribute("non-null")
            this.isList = node.attribute("isList")

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
                        ArrayList<MNode> preDataFetcherNodes = childNode.children("pre-data-fetcher")
                        if (preDataFetcherNodes.size() > 0 ) {
                            this.preDataFetcher = preDataFetcherNodes[0].attribute("service")
                        }
                        ArrayList<MNode> postDataFetcherNodes = childNode.children("post-data-fetcher")
                        if (preDataFetcherNodes.size() > 0) {
                            this.postDataFetcher = postDataFetcherNodes[0].attribute("service")
                        }
                        ArrayList<MNode> serviceNodes = childNode.children("service")
                        if (serviceNodes.size() > 0) {
                            this.dataFetcher = new DataFetcherService(serviceNodes[0])
                        }
                        break
                    case "field":
                        this.fieldList.add(new FieldNode(childNode))
                        break
                }
            }
        }
    }

    static abstract class DataFetcherHandler {

    }

    static class DataFetcherService extends DataFetcherHandler {
        String service

        DataFetcherService(MNode node) {
            this.service = node.attribute("service")
        }
    }

}
