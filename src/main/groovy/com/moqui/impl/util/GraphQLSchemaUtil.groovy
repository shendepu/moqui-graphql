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
package com.moqui.impl.util

import org.moqui.impl.context.ExecutionContextImpl
import org.moqui.impl.entity.FieldInfo
import org.moqui.context.ExecutionContext
import org.moqui.impl.entity.EntityDefinition
import org.moqui.util.MNode
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static org.moqui.impl.entity.EntityDefinition.MasterDefinition
import static org.moqui.impl.entity.EntityDefinition.MasterDetail
import static org.moqui.impl.entity.EntityJavaUtil.RelationshipInfo

import static com.moqui.impl.service.GraphQLSchemaDefinition.ArgumentNode
import static com.moqui.impl.service.GraphQLSchemaDefinition.DataFetcherHandler
import static com.moqui.impl.service.GraphQLSchemaDefinition.DataFetcherEntity
import static com.moqui.impl.service.GraphQLSchemaDefinition.FieldNode
import static com.moqui.impl.service.GraphQLSchemaDefinition.GraphQLTypeNode
import static com.moqui.impl.service.GraphQLSchemaDefinition.ObjectTypeNode

class GraphQLSchemaUtil {
    protected final static Logger logger = LoggerFactory.getLogger(GraphQLSchemaUtil.class)

    static final Map<String, String> fieldTypeGraphQLMap = [
            "id"                : "ID",         "id-long"           : "ID",
            "text-indicator"    : "String",     "text-short"        : "String",
            "text-medium"       : "String",     "text-long"         : "String",
            "text-very-long"    : "String",     "date-time"         : "Timestamp",
            "time"              : "String",     "date"              : "String",
            "number-integer"    : "Int",        "number-float"      : "Float",
            "number-decimal"    : "BigDecimal", "currency-amount"   : "BigDecimal",
            "currency-precise"  : "BigDecimal", "binary-very-long"  : "Byte"
    ] as Map<String, String>

    static final List<String> moquiStringTypes = ["id", "id-long", "text-short", "text-medium", "text-long", "text-very-long"]
    static final List<String> moquiDateTypes = ["date", "time", "date-time"]
    static final List<String> moquiNumericTypes = ["number-integer", "number-float", "number-decimal", "currency-amount", "currency-precise"]
    static final List<String> moquiBoolTypes = ["text-indicator"]

    static void createObjectTypeNodeForAllEntities(ExecutionContext ec, Map<String, GraphQLTypeNode> allTypeNodeMap) {
        ExecutionContextImpl eci = (ExecutionContextImpl) ec
        for (String entityName in eci.getEntityFacade().getAllEntityNames()) {
            EntityDefinition ed = eci.getEntityFacade().getEntityDefinition(entityName)
            addObjectTypeNode(ec, ed, true, "default", null, allTypeNodeMap)
        }
    }

    private static void addObjectTypeNode(ExecutionContext ec, EntityDefinition ed, boolean standalone, String masterName, MasterDetail masterDetail, Map<String, GraphQLTypeNode> allTypeNodeMap) {
        ExecutionContextImpl eci = (ExecutionContextImpl) ec
        String objectTypeName = ed.getEntityName()

        // Check if the type already exist in the map
        if (allTypeNodeMap.get(objectTypeName)) {
//            logger.error("Object type [${objectTypeName}] is already defined. So auto object type for entity [${ed.getFullEntityName()}] can't be created.")
            return
        }

        List<FieldNode> fieldNodeList = new ArrayList<>()

        ArrayList<String> allFields = ed.getAllFieldNames()
        for (String fieldName in allFields) {
            // Add fields in entity as field
            FieldInfo fi = ed.getFieldInfo(fieldName)
            String fieldScalarType = fieldTypeGraphQLMap.get(fi.type)

            Map<String, String> fieldPropertyMap = new HashMap<>()
            // Add nonNull
            if (fi.isPk || "true".equals(fi.fieldNode.attribute("not-null"))) fieldPropertyMap.put("nonNull", "true")
            // Add description
            String fieldDescription = ""
            for (MNode descriptionMNode in fi.fieldNode.children("description")) {
                fieldDescription = fieldDescription + descriptionMNode.text + "\n"
            }
            fieldPropertyMap.put("description", fieldDescription)

            FieldNode fieldNode = new FieldNode(ec, fi.name, fieldScalarType, fieldPropertyMap)
            fieldNodeList.add(fieldNode)

        }

        // Add Master-Detail in entity as field
        MasterDefinition masterDef = ed.getMasterDefinition(masterName)
        List<MasterDetail> detailList = new ArrayList<>()
        if (masterDef) {
            detailList = masterDef.detailList
        }

        for (MasterDetail childMasterDetail in detailList) {
            RelationshipInfo relInfo = childMasterDetail.relInfo
            EntityDefinition relEd = eci.getEntityFacade().getEntityDefinition(relInfo.relatedEntityName)

            String fieldName = childMasterDetail.relationshipName
            String fieldType = relEd.getEntityName()

            Map<String, String> fieldPropertyMap = new HashMap<>()
            if (relInfo.isTypeOne) {
                fieldPropertyMap.put("nonNull", "true")
            } else {
                fieldPropertyMap.put("isList", "true")
            }

            List<ArgumentNode> argumentNodeList = new ArrayList<>()

            if (!relInfo.isTypeOne) {
                logger.info("Adding ArgumentNodes for [${fieldName} - ${fieldType}]")
                for (String fieldNameRel in relEd.getAllFieldNames()) {
                    // Skip the relationship fields
                    if (relInfo.keyMap.values().contains(fieldNameRel)) continue

                    FieldInfo fir = relEd.getFieldInfo(fieldNameRel)
                    String fieldDescription = ""
                    for (MNode descriptionMNode in fir.fieldNode.children("description")) {
                        fieldDescription = fieldDescription + descriptionMNode.text + "\n"
                    }

                    // Add fields in entity as argument
                    ArgumentNode argumentNode
                    if (moquiDateTypes.contains(fir.type)) {
                        argumentNode = new ArgumentNode(fir.name, "GraphQLDateRangeInputType", null, fieldDescription)
                        argumentNodeList.add(argumentNode)
                    } else if (moquiStringTypes.contains(fir.type) || moquiNumericTypes.contains(fir.type) || moquiBoolTypes.contains(fir.type)) {
                        argumentNode = new ArgumentNode(fir.name, "GraphQLOperationInputType", null, fieldDescription)
                        argumentNodeList.add(argumentNode)
                    } else {
                        argumentNode = new ArgumentNode(fir.name, fieldTypeGraphQLMap.get(fir.type), null, fieldDescription)
                        argumentNodeList.add(argumentNode)
                    }

                    argumentNode = new ArgumentNode("pagination", "GraphQLPaginationInputType", null, "Pagination")
                    argumentNodeList.add(argumentNode)

                    argumentNodeList.add(argumentNode)
                }
            }


            logger.info("===== Adding FieldNode [${fieldName} - ${fieldType}]")
            FieldNode fieldNode = new FieldNode(ec, fieldName, fieldType, fieldPropertyMap, argumentNodeList)

            DataFetcherHandler dataFetcher = new DataFetcherEntity(ec, fieldNode, relInfo.relatedEntityName, "list", relInfo.keyMap)
            fieldNode.setDataFetcher(dataFetcher)

            fieldNodeList.add(fieldNode)
        }

        String objectTypeDescription = ""
        for (MNode descriptionMNode in ed.getEntityNode().children("description")) {
            objectTypeDescription = objectTypeDescription + descriptionMNode.text + "\n"
        }

        ObjectTypeNode objectTypeNode = new ObjectTypeNode(ec, objectTypeName, objectTypeDescription, new ArrayList<String>(), fieldNodeList, "")
        allTypeNodeMap.put(objectTypeName, objectTypeNode)
        logger.info("Object type [${objectTypeName}] for entity [${ed.getFullEntityName()}] is created.")
    }
}
