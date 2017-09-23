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

import com.moqui.graphql.DateRangeInputType
import com.moqui.graphql.OperationInputType
import com.moqui.graphql.PaginationInputType
import com.moqui.impl.service.GraphQLSchemaDefinition
import com.moqui.impl.service.fetcher.BaseDataFetcher
import com.moqui.impl.service.fetcher.EmptyDataFetcher
import com.moqui.impl.service.fetcher.EntityBatchedDataFetcher
import com.moqui.impl.service.fetcher.ServiceDataFetcher
import graphql.language.Field
import graphql.schema.DataFetchingEnvironment
import graphql.schema.GraphQLScalarType
import groovy.transform.CompileStatic
import org.apache.commons.codec.digest.DigestUtils
import org.moqui.context.ExecutionContext
import org.moqui.context.ExecutionContextFactory
import org.moqui.entity.EntityException
import org.moqui.entity.EntityFind
import org.moqui.entity.EntityList
import org.moqui.entity.EntityValue
import org.moqui.impl.context.ExecutionContextFactoryImpl
import org.moqui.impl.context.ExecutionContextImpl
import org.moqui.impl.entity.EntityDataDocument
import org.moqui.impl.entity.EntityFacadeImpl
import org.moqui.impl.entity.FieldInfo
import org.moqui.impl.entity.EntityDefinition
import org.moqui.impl.service.ServiceDefinition
import org.moqui.util.MNode
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.sql.Timestamp
import java.util.concurrent.atomic.AtomicBoolean

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

import static com.moqui.impl.service.GraphQLSchemaDefinition.GraphQLTypeDefinition
import static com.moqui.impl.service.GraphQLSchemaDefinition.ArgumentDefinition
import static com.moqui.impl.service.GraphQLSchemaDefinition.AutoArgumentsDefinition
import static com.moqui.impl.service.GraphQLSchemaDefinition.ObjectTypeDefinition
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

    static final Map<String, String> javaTypeGraphQLMap = [
            "String"        : "String",     "java.lang.String"          : "String",
            "CharSequence"  : "String",     "java.lang.CharSequence"    : "String",
            "Date"          : "String",     "java.sql.Date"             : "String",
            "Time"          : "String",     "java.sql.Time"             : "String",
            "Timestamp"     : "Timestamp",  "java.sql.Timestamp"        : "Timestamp",
            "Integer"       : "Int",        "java.lang.Integer"         : "Int",
            "Long"          : "Long",       "java.lang.Long"            : "Long",
            "BigInteger"    : "BigInteger", "java.math.BigInteger"      : "BigInteger",
            "Float"         : "Float",      "java.lang.Float"           : "Float",
            "Double"        : "Float",      "java.lang.Double"          : "Float",
            "BigDecimal"    : "BigDecimal", "java.math.BigDecimal"      : "BigDecimal",
            "Boolean"       : "Boolean",    "java.lang.Boolean"         : "Boolean"
    ] as Map<String, String>

    static final List<String> moquiStringTypes = ["id", "id-long", "text-short", "text-medium", "text-long", "text-very-long"]
    static final List<String> moquiDateTypes = ["date", "time", "date-time"]
    static final List<String> moquiNumericTypes = ["number-integer", "number-float", "number-decimal", "currency-amount", "currency-precise"]
    static final List<String> moquiBoolTypes = ["text-indicator"]

    // this is reverse mapping between elasticsearch type and moqui entity field type.
    // used by `getDataDocDefinition` only due to data document field which is calculation, And it has a fieldType
    // defining the type in elasticsearch types, but we need the entity field type instead.
    static final Map<String, String> esTypeEntityMap = [
            "keyword": "text-medium",
            "date": "date",
            "text": "text-medium",
            "integer": "number-integer",
            "long": "number-integer",
            "float": "number-float",
            "double": "number-decimal",
            "binary": "binary-very-long"
    ]

    static String getGraphQLTypeNameByJava(String javaType) {
        if (!javaType) return "String"
        return javaTypeGraphQLMap.get(getShortJavaType(javaType))
    }
    
    static String getShortJavaType(String javaType) {
        if (!javaType) return ""
        String shortJavaType = javaType
        if (javaType.contains(".")) shortJavaType = javaType.substring(javaType.lastIndexOf(".") + 1)
        return shortJavaType
    }

    static void createObjectTypeNodeForAllEntities(ExecutionContextFactory ecf, Map<String, GraphQLTypeDefinition> allTypeNodeMap) {
        for (String entityName in ((ExecutionContextFactoryImpl) ecf).entityFacade.getAllEntityNames()) {
            EntityDefinition ed = ((ExecutionContextFactoryImpl) ecf).entityFacade.getEntityDefinition(entityName)
            addObjectTypeNode(ecf, ed, true, "default", null, allTypeNodeMap)
        }
    }

    private static void addObjectTypeNode(ExecutionContextFactory ecf, EntityDefinition ed, boolean standalone, String masterName, MasterDetail masterDetail,
                                          Map<String, GraphQLTypeDefinition> allTypeDefMap) {
        String objectTypeName = ed.getEntityName()

        // Check if the type already exist in the map
        if (allTypeDefMap.get(objectTypeName)) return

        Map<String, FieldDefinition> fieldDefMap = new LinkedHashMap<>()

        List<String> allFields = ed.getAllFieldNames()

        if (!allFields.contains("id")) {
            // Add a id field to all entity Object Type
            FieldDefinition idFieldDef = GraphQLSchemaDefinition.getCachedFieldDefinition("id", "ID", "false", "false", "false")
            if (idFieldDef == null) {
                idFieldDef = new FieldDefinition(ecf, "id", "ID", [:])
                GraphQLSchemaDefinition.putCachedFieldDefinition(idFieldDef)
            }
            fieldDefMap.put("id", idFieldDef)
        }

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
            } else {
                fieldPropertyMap.put("isList", "true")
            }

            List<String> excludedArguments = new ArrayList<>()
            excludedArguments.addAll(relInfo.keyMap.values())
//            if (relInfo.relatedEntityName.equals(ed.getFullEntityName())) excludedArguments.addAll(relInfo.keyMap.keySet())
            if (relInfo.type.startsWith("one")) excludedArguments.addAll(relEd.getPkFieldNames())
            FieldDefinition fieldDef = new FieldDefinition(ecf, fieldName, fieldType, fieldPropertyMap, excludedArguments)

            BaseDataFetcher dataFetcher = new EntityBatchedDataFetcher(ecf, fieldDef, relInfo.relatedEntityName, relInfo.keyMap)
            fieldDef.setDataFetcher(dataFetcher)

            fieldDefMap.put(fieldName, fieldDef)
        }

        String objectTypeDescription = ""
        for (MNode descriptionMNode in ed.getEntityNode().children("description")) {
            objectTypeDescription = objectTypeDescription + descriptionMNode.text + "\n"
        }

        ObjectTypeDefinition objectTypeDef = new ObjectTypeDefinition(ecf, objectTypeName, objectTypeDescription, new ArrayList<String>(), fieldDefMap)
        allTypeDefMap.put(objectTypeName, objectTypeDef)
    }

    static String getEntityFieldGraphQLType(String type) {
        return fieldTypeGraphQLMap.get(type)
    }

    static void createObjectTypeNodeForAllDataDocuments(ExecutionContextFactory ecf, Map<String, GraphQLTypeDefinition> allTypeNodeMap) {
        boolean alreadyDisabled = ecf.executionContext.artifactExecution.disableAuthz()
        try {
            EntityList el = ecf.entity.find("moqui.entity.feed.DataFeedDocument").selectField("dataDocumentId").distinct(true).useCache(true).list()

            el.collect { EntityValue ev -> addObjectTypeNode(ecf, ev.get("dataDocumentId") as String, allTypeNodeMap) }
        } finally {
            if (!alreadyDisabled) ecf.executionContext.artifactExecution.enableAuthz()
        }
    }

    private static void addObjectTypeNode(ExecutionContextFactory ecf, String dataDocumentId, Map<String, GraphQLTypeDefinition> allTypeDefMap) {
        String objectTypeName = "FT" + dataDocumentId
        // Check if the type already exist in the map
        if (allTypeDefMap.get(objectTypeName)) return

        Map<String, Object> dataDocDefMap = getDataDocDefinition(ecf, dataDocumentId)

        addNestFTObjectTypeNode(ecf, objectTypeName, dataDocDefMap, allTypeDefMap)
    }

    private static void addNestFTObjectTypeNode(ExecutionContextFactory ecf, String objectTypeName, Map<String, Object> dataDocDefCurrent,
                                                Map<String, GraphQLTypeDefinition> allTypeDefMap) {
        // Check if the type already exist in the map
        if (allTypeDefMap.get(objectTypeName)) return

        Map<String, FieldDefinition> fieldDefMap = new LinkedHashMap<>()
        for (Map.Entry<String, Object> dataDocDefEntry in dataDocDefCurrent) {
            String fieldName = dataDocDefEntry.key
            if (dataDocDefEntry.value instanceof Map) {
                String fieldType = fieldName
                Map<String, Object> dataDocDefChild = dataDocDefEntry.value as Map
                String childObjectTypeName = objectTypeName + camelCaseToUpperCamel(fieldType)
                addNestFTObjectTypeNode(ecf, childObjectTypeName, dataDocDefChild, allTypeDefMap)

                FieldDefinition fieldDef = new FieldDefinition(ecf, fieldName, childObjectTypeName, [isList: "true"])

                fieldDefMap.put(fieldName, fieldDef)

            } else {
                String fieldScalarType = fieldTypeGraphQLMap.get(dataDocDefEntry.value)
                if (fieldScalarType == null) throw new IllegalArgumentException("Can't map entity field type ${dataDocDefEntry.value} to GraphQL type")

                FieldDefinition fieldDef = GraphQLSchemaDefinition.getCachedFieldDefinition(fieldName, fieldScalarType, "false", "false", "false")
                if (fieldDef == null) {
                    fieldDef = new FieldDefinition(ecf, fieldName, fieldScalarType, [:])
                    GraphQLSchemaDefinition.putCachedFieldDefinition(fieldDef)
                }
                fieldDefMap.put(fieldName, fieldDef)
            }
        }
        ObjectTypeDefinition objectTypeDef = new ObjectTypeDefinition(ecf, objectTypeName, "", new ArrayList<String>(), fieldDefMap)
        allTypeDefMap.put(objectTypeName, objectTypeDef)
    }

    static Map<String, Object> getDataDocDefinition(ExecutionContextFactory ecf, String dataDocumentId) {
        ExecutionContextImpl eci = (ecf as ExecutionContextFactoryImpl).getEci()
        EntityFacadeImpl efi = eci.entityFacade

        EntityValue dataDocument = efi.fastFindOne("moqui.entity.document.DataDocument", true, false, dataDocumentId)
        if (dataDocument == null) throw new EntityException("No DataDocument found with ID [${dataDocumentId}]")
        EntityList dataDocumentFieldList = dataDocument.findRelated("moqui.entity.document.DataDocumentField", null, null, true, false)
        EntityList dataDocumentRelAliasList = dataDocument.findRelated("moqui.entity.document.DataDocumentRelAlias", null, null, true, false)

        Map<String, String> relationshipAliasMap = [:]
        for (EntityValue dataDocumentRelAlias in dataDocumentRelAliasList)
            relationshipAliasMap.put((String) dataDocumentRelAlias.relationshipName, (String) dataDocumentRelAlias.documentAlias)

        String primaryEntityName = dataDocument.primaryEntityName
        EntityDefinition primaryEd = efi.getEntityDefinition(primaryEntityName)

        Map<String, Object> docDefMap = [:]
        docDefMap.put("_id", "id")
        docDefMap.put("id", "id")

        List<String> remainingPKFields = new ArrayList<>(primaryEd.getPkFieldNames())
        for (EntityValue dataDocumentField in dataDocumentFieldList) {
            String fieldPath = (String) dataDocumentField.fieldPath
            ArrayList<String> fieldPathElementList = EntityDataDocument.fieldPathToList(fieldPath)
            if (fieldPathElementList.size() == 1) {
                String fieldName = ((String) dataDocumentField.fieldNameAlias) ?: fieldPath
                String esFieldType = (String) dataDocumentField.fieldType ?: "double" // in moqui-elasticsearch, the default type is `double`
                if (fieldPath.startsWith("(")) {
                    if (!esFieldType) throw new IllegalArgumentException("Could not find fieldType for field [${fieldName}]")
                    String fieldType = esTypeEntityMap.get(esFieldType)
                    if (!fieldType) throw new IllegalArgumentException("Could not find entity field type for elasticsearch type [${esFieldType}]")

                    docDefMap.put(fieldName, fieldType)
                } else {
                    FieldInfo fieldInfo = primaryEd.getFieldInfo(fieldPath)
                    if (fieldInfo == null) throw new EntityException("Could not find field [${fieldPath}] for entity [${primaryEd.getFullEntityName()}] in DataDocument [${dataDocumentId}]")
                    docDefMap.put(fieldName, fieldInfo.type)
                    if (remainingPKFields.contains(fieldPath)) remainingPKFields.remove(fieldPath)
                }
                
                continue
            }

            Map<String, Object> currentDocDefMap = docDefMap
            EntityDefinition currentEd = primaryEd
            int fieldPathElementListSize = fieldPathElementList.size()
            for (int i = 0; i < fieldPathElementListSize; i++) {
                String fieldPathElement = (String) fieldPathElementList.get(i)
                if (i < (fieldPathElementListSize - 1)) {
                    RelationshipInfo relInfo = currentEd.getRelationshipInfo(fieldPathElement)
                    if (relInfo == null) throw new EntityException("Could not find relationship [${fieldPathElement}] for entity [${currentEd.getFullEntityName()}] in DataDocument [${dataDocumentId}]")
                    currentEd = relInfo.relatedEd
                    if (currentEd == null) throw new EntityException("Could not find entity [${relInfo.relatedEntityName}] in DataDocument [${dataDocumentId}]")

                    // only put type many in sub-objects, same as DataDocument generation
                    if (!relInfo.isTypeOne) {
                        String objectName = relationshipAliasMap.get(fieldPathElement) ?: fieldPathElement
                        Map<String, Object> subDocDefMap = (Map<String, Object>) currentDocDefMap.get(objectName)
                        if (subDocDefMap == null) subDocDefMap = new HashMap<>()

                        currentDocDefMap.put(objectName, subDocDefMap)
                        currentDocDefMap = subDocDefMap
                    }
                } else {
                    String fieldName = (String) dataDocumentField.fieldNameAlias ?: fieldPathElement
                    String esFieldType = (String) dataDocumentField.fieldType ?: "double" // in moqui-elasticsearch, the default type is `double`
                    if (fieldPathElement.startsWith("(")) {
                        if (!esFieldType) throw new IllegalArgumentException("Could not find fieldType for field [${fieldName}]")
                        String fieldType = esTypeEntityMap.get(esFieldType)
                        if (!fieldType) throw new IllegalArgumentException("Could not find entity field type for elasticsearch type [${esFieldType}]")

                        currentDocDefMap.put(fieldName, fieldType)
                    } else {
                        FieldInfo fieldInfo = currentEd.getFieldInfo(fieldPathElement)
                        if (fieldInfo == null) throw new EntityException("Could not find field [${fieldPathElement}] for entity [${currentEd.getFullEntityName()}] in DataDocument [${dataDocumentId}]")
                        currentDocDefMap.put(fieldName, fieldInfo.type)
                    }
                }
            }
        }

        for (String remainingPkName in remainingPKFields) {
            FieldInfo fieldInfo = primaryEd.getFieldInfo(remainingPkName)
            docDefMap.put(remainingPkName, fieldInfo.type)
        }

        return docDefMap
    }

    static void mergeFieldDefinition(MNode fieldNode, Map<String, FieldDefinition> fieldDefMap, ExecutionContextFactory ecf) {
        FieldDefinition fieldDef = fieldDefMap.get(fieldNode.attribute("name"))
        if (fieldDef != null) {
            if (fieldNode.attribute("type")) fieldDef.type = fieldNode.attribute("type")
            if (fieldNode.attribute("non-null")) fieldDef.nonNull = fieldNode.attribute("non-null")
            if (fieldNode.attribute("is-list")) fieldDef.isList = fieldNode.attribute("is-list")
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
                        fieldDef.setDataFetcher(new EntityBatchedDataFetcher(childNode, fieldDef, ecf))
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

    static Object castValueToJavaType(Object value, String javaType) {
        switch (javaType) {
            case "String": return value as String
            case "CharSequence": return value as String
            case "Date": break  //TODO
            case "Time": break  //TODO
            case "Timestamp": return value as Timestamp
            case "Integer": return value as Integer
            case "Long": return value as Long
            case "BigInteger": return value as BigInteger
            case "Float": return value as Float
            case "Double": return value as Double
            case "BigDecimal": return value as BigDecimal
            case "Boolean": return value as Boolean
            case "List": return value as List
            case "Map": return value as Map
            default:
                throw new IllegalArgumentException("Can't cast value [${value}] to Java type ${javaType}")
                break
        }
    }


    static boolean requirePagination(DataFetchingEnvironment environment) {
        List<Map> sources = (List<Map>) environment.source

        Map<String, Object> arguments = (Map) environment.arguments
        List<Field> fields = (List) environment.fields
        Map paginationArg = arguments.get("pagination") as Map
        if (paginationArg?.get("pageNoLimit")) return false
        if (paginationArg != null) return true
        if (fields.find({ it.name == "pageInfo" }) != null) return true
        if (sources.size() == 1 && sources.get(0).size() == 0) return true
        return false
    }

    static void transformQueryServiceRelArguments(Map<String, Object> source, Map<String, String> relKeyMap, Map<String, Object> inParameterMap) {
        for (Map.Entry<String, Object> keyMapEntry in relKeyMap)
            inParameterMap.put(keyMapEntry.value as String, source.get(keyMapEntry.key))
    }

    static void transformQueryServiceArguments(ServiceDefinition sd, Map<String, Object> arguments, Map<String, Object> inParameterMap) {
        for (Map.Entry<String, Object> entry in arguments.entrySet()) {
            String paramName = entry.key
            if ("if".equals(paramName)) continue
            if (entry.value == null) continue
            MNode paramNode = sd.getInParameter(paramName)
            if (paramNode == null) throw new IllegalArgumentException("Service ${sd.serviceName} missing in parameter ${paramName}")
            String paramType = paramNode.attribute("type") ?: "String"
            Object paramJavaTypeValue
            logger.info("argument: ${paramName} - ${entry.value}")
            switch (paramType) {
                case "com.moqui.graphql.OperationInputType":
                    paramJavaTypeValue = new OperationInputType(entry.value as Map); break
                case "com.moqui.graphql.DateRangeInputType":
                    paramJavaTypeValue = new DateRangeInputType(entry.value as Map); break
                case "com.moqui.graphql.PaginationInputType":
                    paramJavaTypeValue = new PaginationInputType(entry.value as Map); break
                default:
                    paramJavaTypeValue = castValueToJavaType(entry.value, paramType); break
            }
            inParameterMap.put(paramName, paramJavaTypeValue)
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
                Map argValueMap = (LinkedHashMap) argValue
                if (argValueMap.get("value") != null) inputFieldsMap.put(argName, argValueMap.get("value"))
                if (argValueMap.get("op") != null) inputFieldsMap.put(argName + "_op", argValueMap.get("op"))
                if (argValueMap.get("not") != null) inputFieldsMap.put(argName + "_not", argValueMap.get("not"))
                if (argValueMap.get("ic") != null) inputFieldsMap.put(argName + "_ic", argValueMap.get("ic"))
                inputFieldsMap.put("pageIndex", argValueMap.get("pageIndex") ?: 0)
                inputFieldsMap.put("pageSize", argValueMap.get("pageSize") ?: 20)
                if (argValueMap.get("pageNoLimit") != null) inputFieldsMap.put("pageNoLimit", argValueMap.get("pageNoLimit"))
                if (argValueMap.get("orderByField") != null) inputFieldsMap.put("orderByField", argValueMap.get("orderByField"))

                if (argValueMap.get("period") != null) inputFieldsMap.put(argName + "_period", argValueMap.get("period"))
                if (argValueMap.get("poffset") != null) inputFieldsMap.put(argName + "_poffset", argValueMap.get("poffset"))
                if (argValueMap.get("from") != null) inputFieldsMap.put(argName + "_from", argValueMap.get("from"))
                if (argValueMap.get("thru") != null) inputFieldsMap.put(argName + "_thru", argValueMap.get("thru"))

            } else {
                // periodValid_ type argument is handled specially
                if (!(argName == "periodValid_" || argName.endsWith("PeriodValid_"))) {
                    inputFieldsMap.put(argName, argValue)
                }
            }
        }
    }

    static void addPeriodValidArguments(ExecutionContext ec, EntityFind ef, Map<String, Object> arguments) {
        for (Map.Entry<String, Object> entry in arguments.entrySet()) {
            String argName = entry.getKey()
            if (!(argName == "periodValid_" || argName.endsWith("PeriodValid_"))) continue

            String fromFieldName = "fromDate"
            String thruFieldName = "thruDate"
            if (argName.endsWith("PeriodValid_")) {
                // 12 = "PeriodValid_".length
                String prefix = argName.substring(0, argName.length() - 12)
                fromFieldName = prefix + "FromDate"
                thruFieldName = prefix + "ThruDate"
            }
            ef.condition(ec.entity.conditionFactory.makeConditionDate(fromFieldName, thruFieldName, ec.user.nowTimestamp))
        }
    }

    static String encodeRelayCursor(EntityValue ev, List<String> pkFieldNames) {
        return encodeRelayId(ev.getMap(), pkFieldNames)
    }

    static String encodeRelayCursor(Map<String, Object> ev, List<String> pkFieldNames) {
        return encodeRelayId(ev, pkFieldNames)
    }

    static String encodeRelayId(EntityValue ev, List<String> pkFieldNames) {
        return encodeRelayId(ev.getMap(), pkFieldNames)
    }

    static String encodeRelayId(Map<String, Object> ev, List<String> pkFieldNames) {
        if (pkFieldNames.size() == 0) throw new IllegalArgumentException("Entity value must have primary keys to generate id")
        String id = ev.get(pkFieldNames[0])
        for (int i = 1; i < pkFieldNames.size(); i++) id = id + '|' + ev.get(pkFieldNames[i])
        return id
    }

    static String camelCaseToUpperCamel(String camelCase) {
        if (camelCase == null || camelCase.length() == 0) return ""

        return Character.toString(Character.toUpperCase(camelCase.charAt(0))) + camelCase.substring(1)
    }

    static String sha1HexShort(String text) {
        return sha1HexShort(text, 7)
    }

    static String sha1HexShort(String text, int length) {
        return DigestUtils.sha1Hex(text.getBytes()).substring(0, length)
    }
}