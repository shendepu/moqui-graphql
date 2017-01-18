package com.moqui.impl.service.fetcher

import graphql.language.Field
import graphql.language.FragmentSpread
import graphql.language.InlineFragment
import graphql.language.Selection
import graphql.language.SelectionSet
import org.moqui.context.ExecutionContext

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
}
