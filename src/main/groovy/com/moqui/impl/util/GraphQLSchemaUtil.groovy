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
import graphql.language.ObjectField
import org.moqui.entity.EntityValue
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

    protected static final Map<String, String> objectTypeGraphQLMap = [
            Integer   : "Int", Long: "Long", Short: "Short", Float: "Float", Double: "Float",
            BigDecimal: "BigDecimal", BigInteger: "BigInteger", Boolean: "Boolean", List: "List",
            Map       : "Map"]

    static String getGraphQLType(String javaType) {
        if (!javaType) return "String"
        if (javaType.contains(".")) javaType = javaType.substring(javaType.lastIndexOf(".") + 1)
        return objectTypeGraphQLMap.get(javaType) ?: "String"
    }

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
        if (allTypeDefMap.get(objectTypeName)) return

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
            if (relInfo.type.startsWith("one")) {
                if ("one".equals(relInfo.type)) fieldPropertyMap.put("nonNull", "true")
            } else {
                fieldPropertyMap.put("isList", "true")
            }

            List<String> excludedArguments = new ArrayList<>()
            excludedArguments.addAll(relInfo.keyMap.values())
//            if (relInfo.relatedEntityName.equals(ed.getFullEntityName())) excludedArguments.addAll(relInfo.keyMap.keySet())
            if (relInfo.type.startsWith("one")) excludedArguments.addAll(relEd.getPkFieldNames())
            FieldDefinition fieldDef = new FieldDefinition(ec, fieldName, fieldType, fieldPropertyMap, excludedArguments)

            DataFetcherHandler dataFetcher = new DataFetcherEntity(ec, fieldDef, relInfo.relatedEntityName, relInfo.keyMap)
            fieldDef.setDataFetcher(dataFetcher)

            fieldDefMap.put(fieldName, fieldDef)
        }

        String objectTypeDescription = ""
        for (MNode descriptionMNode in ed.getEntityNode().children("description")) {
            objectTypeDescription = objectTypeDescription + descriptionMNode.text + "\n"
        }

        GraphQLSchemaDefinition.ObjectTypeDefinition objectTypeDef = new GraphQLSchemaDefinition.ObjectTypeDefinition(ec, objectTypeName, objectTypeDescription, new ArrayList<String>(), fieldDefMap)
        allTypeDefMap.put(objectTypeName, objectTypeDef)
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

    static void transformArguments(Map<String, Object> arguments, Map<String, Object> inputFieldsMap) {
        for (Map.Entry<String, Object> entry in arguments.entrySet()) {
            String argName = entry.getKey()
            // Ignore if argument which is used for directive @include and @skip
            if ("if".equals(argName)) continue
            Object argValue = entry.getValue()
            if (argValue == null) continue

            if (argValue instanceof LinkedHashMap) {
                if ("input".equals(argName)) {
                    for (Map.Entry<Object, Object> inputEntry in ((LinkedHashMap) argValue)) {
                        inputFieldsMap.put((String) inputEntry.getKey(), inputEntry.getValue())
                    }
                    continue
                }
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

    public static String base64EncodeCursor(EntityValue ev, String fieldRawType, List<String> pkFieldNames) {
        String cursor = fieldRawType
        for (String pk in pkFieldNames) cursor = cursor + '|' + ev.get(pk)
        return Base64.getEncoder().encodeToString(cursor.bytes)
    }

    public static String camelCaseToUpperCamel(String camelCase) {
        if (camelCase == null || camelCase.length() == 0) return ""
        return camelCase.replace(camelCase.charAt(0), Character.toUpperCase(camelCase.charAt(0)))
    }
}
