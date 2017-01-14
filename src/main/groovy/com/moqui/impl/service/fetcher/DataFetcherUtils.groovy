package com.moqui.impl.service.fetcher

import graphql.language.Field
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

    static ArrayList<String> getActualLocalizeFields(SelectionSet selectionSet, List<String> localizeFields, boolean isList) {
        List<String> actualLocalizedFields = new ArrayList<>()
        if (selectionSet == null) return actualLocalizedFields

        SelectionSet finalSelectionSet
        if (isList) {
            Selection edgesSS = selectionSet.selections.find { Selection selection ->
                return selection instanceof Field && (selection as Field).name == "edges"
            }
            finalSelectionSet = (edgesSS as Field).selectionSet

            Selection nodeSS = finalSelectionSet.selections.find { Selection selection ->
                return selection instanceof Field && (selection as Field).name == "node"
            }
            finalSelectionSet = (nodeSS as Field).selectionSet
        } else {
            finalSelectionSet = selectionSet
        }

        finalSelectionSet.selections.collect { Selection selection ->
            if (selection instanceof Field) {
                Field field = selection as Field
                if (localizeFields.contains(field.name)) actualLocalizedFields.add(field.name)
            }
        }

        return actualLocalizedFields
    }
}
