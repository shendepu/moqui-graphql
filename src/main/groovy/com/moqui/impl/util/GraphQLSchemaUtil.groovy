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
import com.moqui.impl.service.fetcher.BaseDataFetcher
import com.moqui.impl.service.fetcher.EmptyDataFetcher
import com.moqui.impl.service.fetcher.EntityDataFetcher
import com.moqui.impl.service.fetcher.ServiceDataFetcher
import graphql.schema.GraphQLScalarType
import groovy.transform.CompileStatic
import org.moqui.context.ExecutionContextFactory
import org.moqui.entity.EntityValue
import org.moqui.impl.context.ExecutionContextFactoryImpl
import org.moqui.impl.entity.FieldInfo
import org.moqui.impl.entity.EntityDefinition
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

import static org.moqui.impl.entity.EntityDefinition.MasterDefinition
import static org.moqui.impl.entity.EntityDefinition.MasterDetail
import static org.moqui.impl.entity.EntityJavaUtil.RelationshipInfo

import static com.moqui.impl.service.GraphQLSchemaDefinition.ArgumentDefinition
import static com.moqui.impl.service.GraphQLSchemaDefinition.AutoArgumentsDefinition
import static com.moqui.impl.service.GraphQLSchemaDefinition.FieldDefinition

@CompileStatic
class GraphQLSchemaUtil {
    protected final static Logger logger = LoggerFactory.getLogger(GraphQLSchemaUtil.class)


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

    static void createObjectTypeNodeForAllEntities(ExecutionContextFactory ecf, Map<String, GraphQLSchemaDefinition.GraphQLTypeDefinition> allTypeNodeMap) {
        for (String entityName in ((ExecutionContextFactoryImpl) ecf).entityFacade.getAllEntityNames()) {
            EntityDefinition ed = ((ExecutionContextFactoryImpl) ecf).entityFacade.getEntityDefinition(entityName)
            addObjectTypeNode(ecf, ed, true, "default", null, allTypeNodeMap)
        }
    }

    private static void addObjectTypeNode(ExecutionContextFactory ecf, EntityDefinition ed, boolean standalone, String masterName, MasterDetail masterDetail, Map<String, GraphQLSchemaDefinition
            .GraphQLTypeDefinition> allTypeDefMap) {
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


            FieldDefinition fieldDef = GraphQLSchemaDefinition.getCachedFieldDefinition(fi.name, fieldScalarType, fieldPropertyMap.get("nonNull"), "false", "false")
            if (fieldDef == null) {
                fieldDef = new FieldDefinition(ecf, fi.name, fieldScalarType, fieldPropertyMap)
                GraphQLSchemaDefinition.putCachedFieldDefinition(fieldDef)
            }
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
            EntityDefinition relEd = ((ExecutionContextFactoryImpl) ecf).entityFacade.getEntityDefinition(relInfo.relatedEntityName)

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
            FieldDefinition fieldDef = new FieldDefinition(ecf, fieldName, fieldType, fieldPropertyMap, excludedArguments)

            BaseDataFetcher dataFetcher = new EntityDataFetcher(ecf, fieldDef, relInfo.relatedEntityName, relInfo.keyMap)
            fieldDef.setDataFetcher(dataFetcher)

            fieldDefMap.put(fieldName, fieldDef)
        }

        String objectTypeDescription = ""
        for (MNode descriptionMNode in ed.getEntityNode().children("description")) {
            objectTypeDescription = objectTypeDescription + descriptionMNode.text + "\n"
        }

        GraphQLSchemaDefinition.ObjectTypeDefinition objectTypeDef = new GraphQLSchemaDefinition.ObjectTypeDefinition(ecf, objectTypeName, objectTypeDescription, new ArrayList<String>(), fieldDefMap)
        allTypeDefMap.put(objectTypeName, objectTypeDef)
    }

    static String getEntityFieldGraphQLType(String type) {
        return fieldTypeGraphQLMap.get(type)
    }


    static void mergeFieldDefinition(MNode fieldNode, Map<String, FieldDefinition> fieldDefMap, ExecutionContextFactory ecf) {
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
                        String argTypeName = GraphQLSchemaDefinition.getArgumentTypeName(childNode.attribute("type"), fieldDef.isList)
                        ArgumentDefinition argDef = GraphQLSchemaDefinition.getCachedArgumentDefinition(childNode.attribute("name"), argTypeName, childNode.attribute("required"))
                        if (argDef == null) {
                            argDef = new ArgumentDefinition(childNode, fieldDef)
                            GraphQLSchemaDefinition.putCachedArgumentDefinition(argDef)
                        }

                        fieldDef.mergeArgument(argDef)
                        break
                    case "empty-fetcher":
                        fieldDef.setDataFetcher(new EmptyDataFetcher(childNode, fieldDef))
                        break
                    case "entity-fetcher":
                        fieldDef.setDataFetcher(new EntityDataFetcher(childNode, fieldDef, ecf))
                        break
                    case "service-fetcher":
                        fieldDef.setDataFetcher(new ServiceDataFetcher(childNode, fieldDef, ecf))
                        break
                    case "pre-fetcher":
                        break
                    case "post-fetcher":
                        break
                }
            }
        } else {
            fieldDef = new FieldDefinition(fieldNode, ecf)
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
