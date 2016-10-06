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
import org.slf4j.Logger
import org.slf4j.LoggerFactory


import static com.moqui.impl.service.GraphQLSchemaDefinition.ArgumentNode
import static com.moqui.impl.service.GraphQLSchemaDefinition.DataFetcherHandler
import static com.moqui.impl.service.GraphQLSchemaDefinition.FieldNode
import static com.moqui.impl.service.GraphQLSchemaDefinition.GraphQLTypeNode
import static com.moqui.impl.service.GraphQLSchemaDefinition.ObjectTypeNode

class GraphQLSchemaUtil {
    protected final static Logger logger = LoggerFactory.getLogger(GraphQLSchemaUtil.class)

    static final Map<String, String> fieldTypeGraphQLMap = [
            "id"                : "ID",         "id-long"           : "ID",
            "text-indicator"    : "String",     "text-short"        : "String",
            "text-medium"       : "String",     "text-long"         : "String",
            "text-very-long"    : "String",     "date-time"         : "String",
            "time"              : "String",     "date"              : "String",
            "number-integer"    : "Int",        "number-float"      : "Float",
            "number-decimal"    : "BigDecimal", "currency-amount"   : "BigDecimal",
            "currency-precise"  : "BigDecimal", "binary-very-long"  : "Byte"
    ]

    static void createObjectTypeNodeForAllEntities(ExecutionContext ec, Map<String, GraphQLTypeNode> allTypeNodeMap) {
        ExecutionContextImpl eci = (ExecutionContextImpl) ec
        for (String entityName in eci.getEntityFacade().getAllEntityNames()) {
            EntityDefinition ed = eci.getEntityFacade().getEntityDefinition(entityName)
            addObjectTypeNode(ec, ed, allTypeNodeMap)
        }
    }

    private static void addObjectTypeNode(ExecutionContext ec, EntityDefinition ed, Map<String, GraphQLTypeNode> allTypeNodeMap) {
        String objectTypeName = ed.getEntityName()

        // Check if the type already exist in the map
        if (allTypeNodeMap.get(objectTypeName)) {
            logger.error("Object type [${objectTypeName}] is already defined. So auto object type for entity [${ed.getFullEntityName()}] can't be created.")
            return
        }

        List<FieldNode> fieldNodeList = new ArrayList<>()
        List<ArgumentNode> argumentNodeList = new ArrayList<>()

        ArrayList<String> allFields = ed.getAllFieldNames()
        for (String fieldName in allFields) {
            FieldInfo fi = ed.getFieldInfo(fieldName)
            String fieldScalarType = fieldTypeGraphQLMap.get(fi.type)

            FieldNode fieldNode = new FieldNode(ec, fi.name, fieldScalarType)
            fieldNodeList.add(fieldNode)
        }

        ObjectTypeNode objectTypeNode = new ObjectTypeNode(ec, objectTypeName, new ArrayList<String>(), fieldNodeList, "")
        allTypeNodeMap.put(objectTypeName, objectTypeNode)
        logger.info("Object type [${objectTypeName}] for entity [${ed.getFullEntityName()}] is created.")
    }
}
