package com.moqui.impl.service.fetcher

import graphql.language.Field
import graphql.language.FragmentSpread
import graphql.language.InlineFragment
import graphql.language.Selection
import graphql.language.SelectionSet
import org.moqui.context.ExecutionContext
import org.moqui.entity.EntityCondition
import org.moqui.entity.EntityCondition.ComparisonOperator
import org.moqui.entity.EntityCondition.JoinOperator
import org.moqui.entity.EntityFind
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class DataFetcherUtils {
    protected final static Logger logger = LoggerFactory.getLogger(DataFetcherUtils.class)

    static void localize(Map data, List<String> actualLocalizedFields, ExecutionContext ec) {
        if (actualLocalizedFields.size() == 0 ) return
        for (String fieldName in actualLocalizedFields) {
            Object fieldValue = data.get(fieldName)
            if (fieldValue == null) break
            if (!(fieldValue instanceof String)) break

            String value = (String) fieldValue
            if (value.isEmpty()) break
            data.put(fieldName, ec.l10n.localize(value))
        }
    }

    static Selection getGraphQLSelection(SelectionSet selectionSet, String name) {
        if (selectionSet == null) return null
        for (Selection selection in selectionSet.selections) {
            if (selection instanceof Field) {
                if ((selection as Field).name == name) return selection
            } else if (selection instanceof FragmentSpread) {
                // Do nothing since FragmentSpread has no way to find selectionSet
            } else if (selection instanceof InlineFragment) {
                getGraphQLSelection((selection as InlineFragment).selectionSet, name)
            }
        }
        return null
    }

    static SelectionSet getGraphQLSelectionSet(Selection selection) {
        if (selection == null) return null
        if (selection instanceof Field) return (selection as Field).selectionSet
        if (selection instanceof InlineFragment) return (selection as InlineFragment).selectionSet
        return null
    }

    static SelectionSet getConnectionNodeSelectionSet(SelectionSet selectionSet) {
        SelectionSet finalSelectionSet

        Selection edgesSS = getGraphQLSelection(selectionSet, "edges")
        finalSelectionSet = getGraphQLSelectionSet(edgesSS)
        if (!finalSelectionSet) return null

        Selection nodeSS = getGraphQLSelection(finalSelectionSet, "node")
        finalSelectionSet = getGraphQLSelectionSet(nodeSS)

        return finalSelectionSet
    }

    static void collectActualLocalizedFields(SelectionSet selectionSet, List<String> localizeFields,
                                             List<String> actualLocalizedFields) {
        if (selectionSet == null) return
        selectionSet.selections.collect { Selection selection ->
            if (selection instanceof Field) {
                Field field = selection as Field
                if (localizeFields.contains(field.name)) actualLocalizedFields.add(field.name)
            } else if (selection instanceof InlineFragment) {
                collectActualLocalizedFields((selection as InlineFragment).selectionSet, localizeFields, actualLocalizedFields)
            } else if (selection instanceof FragmentSpread) {
                // Do nothing for now
            }
        }
    }

    static ArrayList<String> getActualLocalizeFields(SelectionSet selectionSet, List<String> localizeFields, boolean isList) {
        List<String> actualLocalizedFields = new ArrayList<>()
        if (selectionSet == null) return actualLocalizedFields

        SelectionSet finalSelectionSet
        if (isList) {
            finalSelectionSet = getConnectionNodeSelectionSet(selectionSet)
        } else {

            finalSelectionSet = selectionSet
        }

        collectActualLocalizedFields(finalSelectionSet, localizeFields, actualLocalizedFields)

        return actualLocalizedFields
    }


    private static EntityFind patchWithInCondition(EntityFind ef, List<Object> sourceItems, Map<String, String> relKeyMap) {
        if (relKeyMap.size() != 1)
            throw new IllegalArgumentException("pathWithIdsCondition should only be used when there is just one relationship key map")
        int sourceItemCount = sourceItems.size()
        String relParentFieldName, relFieldName
        List<Object> ids = new ArrayList<>(sourceItemCount)
        relParentFieldName = relKeyMap.keySet().asList().get(0)
        relFieldName = relKeyMap.values().asList().get(0)

        for (Object sourceItem in sourceItems) {
            Object relFieldValue = ((Map) sourceItem).get(relParentFieldName)
            if (relFieldValue != null) ids.add(relFieldValue)
        }

        ef.condition(relFieldName, ComparisonOperator.IN, ids)
        return ef
    }

    private static EntityFind patchWithTupleOrCondition(EntityFind ef, List<Object> sourceItems, Map<String, String> relKeyMap, ExecutionContext ec) {
        EntityCondition orCondition = null

        for (Object object in sourceItems) {
            EntityCondition tupleCondition = null
            Map sourceItem = (Map) object
            for (Map.Entry<String, String> entry in relKeyMap.entrySet()) {
                if (tupleCondition == null)
                    tupleCondition = ec.entity.conditionFactory.makeCondition(entry.getValue(), ComparisonOperator.EQUALS, sourceItem.get(entry.getKey()))
                else
                    tupleCondition = ec.entity.conditionFactory.makeCondition(tupleCondition, JoinOperator.AND,
                            ec.entity.conditionFactory.makeCondition(entry.getValue(), ComparisonOperator.EQUALS, sourceItem.get(entry.getKey())))
            }

            if (orCondition == null) orCondition = tupleCondition
            else orCondition = ec.entity.conditionFactory.makeCondition(orCondition, JoinOperator.OR, tupleCondition)
        }

        ef.condition(orCondition)
        return ef
    }

    static EntityFind patchWithConditions(EntityFind ef, List<Object> sourceItems, Map<String, String> relKeyMap, ExecutionContext ec) {
        int relKeyCount = relKeyMap.size()
        if (relKeyCount == 1) {
            patchWithInCondition(ef, sourceItems, relKeyMap)
        } else if (relKeyCount > 1) {
            patchWithTupleOrCondition(ef, sourceItems, relKeyMap, ec)
        }
        return ef
    }

    static EntityFind patchFindOneWithConditions(EntityFind ef, Map sourceItem, Map<String, String> relKeyMap, ExecutionContext ec) {
        if (relKeyMap.size() == 0) return ef
        for (Map.Entry<String, String> entry in relKeyMap.entrySet()) {
            String relParentFieldName = entry.getKey()
            String relFieldName = entry.getValue()
            ef.condition(relFieldName, sourceItem.get(relParentFieldName))
        }
        return ef
    }
    
    static boolean matchParentByRelKeyMap(Map<String, Object> sourceItem, Map<String, Object> self, Map<String, String> relKeyMap) {
        int found = -1
        for (Map.Entry<String, String> entry in relKeyMap.entrySet()) {
            found = (found == -1) ? (sourceItem.get(entry.key) == self.get(entry.value) ? 1 : 0)
                    : (found == 1 && sourceItem.get(entry.key) == self.get(entry.value) ? 1 : 0)
        }
        return found == 1
    }
}
