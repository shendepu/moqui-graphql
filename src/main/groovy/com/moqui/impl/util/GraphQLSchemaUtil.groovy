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

import com.moqui.impl.service.GraphQLSchemaDefinition
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

import static com.moqui.impl.service.GraphQLSchemaDefinition.ArgumentDefinition
import static com.moqui.impl.service.GraphQLSchemaDefinition.AutoArgumentsDefinition
import static com.moqui.impl.service.GraphQLSchemaDefinition.DataFetcherHandler
import static com.moqui.impl.service.GraphQLSchemaDefinition.DataFetcherEntity
import static com.moqui.impl.service.GraphQLSchemaDefinition.FieldDefinition
import static com.moqui.impl.service.GraphQLSchemaDefinition.DataFetcherService

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

    static void createObjectTypeNodeForAllEntities(ExecutionContext ec, Map<String, GraphQLSchemaDefinition.GraphQLTypeDefinition> allTypeNodeMap) {
        ExecutionContextImpl eci = (ExecutionContextImpl) ec
        for (String entityName in eci.getEntityFacade().getAllEntityNames()) {
            EntityDefinition ed = eci.getEntityFacade().getEntityDefinition(entityName)
            addObjectTypeNode(ec, ed, true, "default", null, allTypeNodeMap)
        }
    }

    private static void addObjectTypeNode(ExecutionContext ec, EntityDefinition ed, boolean standalone, String masterName, MasterDetail masterDetail, Map<String, GraphQLSchemaDefinition.GraphQLTypeDefinition> allTypeDefMap) {
        ExecutionContextImpl eci = (ExecutionContextImpl) ec
        String objectTypeName = ed.getEntityName()

        // Check if the type already exist in the map
        if (allTypeDefMap.get(objectTypeName)) {
//            logger.error("Object type [${objectTypeName}] is already defined. So auto object type for entity [${ed.getFullEntityName()}] can't be created.")
            return
        }

        Map<String, FieldDefinition> fieldDefMap = new LinkedHashMap<>()

        List<String> allFields = ed.getAllFieldNames()
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

            FieldDefinition fieldDef = new FieldDefinition(ec, fi.name, fieldScalarType, fieldPropertyMap)
            fieldDefMap.put(fieldName, fieldDef)
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

            List<ArgumentDefinition> argumentDefList = new LinkedList<>()

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
                    ArgumentDefinition argumentDef
                    if (moquiDateTypes.contains(fir.type)) {
                        argumentDef = new ArgumentDefinition(fir.name, "DateRangeInputType", null, null, fieldDescription)
                        argumentDefList.add(argumentDef)
                    } else if (moquiStringTypes.contains(fir.type) || moquiNumericTypes.contains(fir.type) || moquiBoolTypes.contains(fir.type)) {
                        argumentDef = new ArgumentDefinition(fir.name, "OperationInputType", null, null, fieldDescription)
                        argumentDefList.add(argumentDef)
                    } else {
                        argumentDef = new ArgumentDefinition(fir.name, fieldTypeGraphQLMap.get(fir.type), null, null, fieldDescription)
                        argumentDefList.add(argumentDef)
                    }

                    argumentDef = new ArgumentDefinition("pagination", "PaginationInputType", null, null, "Pagination")
                    argumentDefList.add(argumentDef)

                    argumentDefList.add(argumentDef)
                }
            }


            logger.info("===== Adding FieldDefinition [${fieldName} - ${fieldType}]")
            FieldDefinition fieldDef = new FieldDefinition(ec, fieldName, fieldType, fieldPropertyMap, argumentDefList)

            DataFetcherHandler dataFetcher = new DataFetcherEntity(ec, fieldDef, relInfo.relatedEntityName, "list", relInfo.keyMap)
            fieldDef.setDataFetcher(dataFetcher)

            fieldDefMap.put(fieldName, fieldDef)
        }

        String objectTypeDescription = ""
        for (MNode descriptionMNode in ed.getEntityNode().children("description")) {
            objectTypeDescription = objectTypeDescription + descriptionMNode.text + "\n"
        }

        GraphQLSchemaDefinition.ObjectTypeDefinition objectTypeDef = new GraphQLSchemaDefinition.ObjectTypeDefinition(ec, objectTypeName, objectTypeDescription, new ArrayList<String>(), fieldDefMap)
        allTypeDefMap.put(objectTypeName, objectTypeDef)
        logger.info("Object type [${objectTypeName}] for entity [${ed.getFullEntityName()}] is created.")
    }

    static String getEntityFieldGraphQLType(String type) {
        return fieldTypeGraphQLMap.get(type)
    }


    static void mergeFieldDefinition(MNode fieldNode, Map<String, FieldDefinition> fieldDefMap, ExecutionContext ec) {
        FieldDefinition fieldDef = fieldDefMap.get(fieldNode.attribute("name"))
        if (fieldDef != null) {
            if (fieldNode.attribute("type")) fieldDef.type = fieldNode.attribute("type")
            if (fieldNode.attribute("non-null")) fieldDef.nonNull = fieldNode.attribute("non-null")
            if (fieldNode.attribute("is-list")) fieldDef.nonNull = fieldNode.attribute("is-list")
            if (fieldNode.attribute("list-item-non-null")) fieldDef.listItemNonNull = fieldNode.attribute("list-item-non-null")
            if (fieldNode.attribute("require-authentication")) fieldDef.requireAuthentication = fieldNode.attribute("require-authentication")
            for (MNode childNode in fieldNode.children) {
                switch (childNode.name) {
                    case "description":
                        fieldDef.description = childNode.text
                        break
                    case "depreciation-reason":
                        fieldDef.depreciationReason = childNode.text
                        break
                    case "auto-arguments":
                        fieldDef.mergeArgument(new AutoArgumentsDefinition(childNode))
                        break
                    case "argument":
                        fieldDef.mergeArgument(new ArgumentDefinition(childNode, fieldDef))
                        break
                    case "empty-fetcher":
                        fieldDef.setDataFetcher(new GraphQLSchemaDefinition.EmptyDataFetcher(childNode, fieldDef))
                        break
                    case "entity-fetcher":
                        fieldDef.setDataFetcher(new DataFetcherEntity(childNode, fieldDef, ec))
                        break
                    case "service-fetcher":
                        fieldDef.setDataFetcher(new DataFetcherService(childNode, fieldDef, ec))
                        break
                    case "pre-fetcher":
                        break
                    case "post-fetcher":
                        break
                }
            }
        } else {
            fieldDef = new FieldDefinition(fieldNode, ec)
            fieldDefMap.put(fieldDef.name, fieldDef)
        }
    }
}
