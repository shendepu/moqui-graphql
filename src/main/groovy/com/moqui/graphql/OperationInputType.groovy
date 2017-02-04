package com.moqui.graphql

import groovy.transform.CompileStatic

@CompileStatic
class OperationInputType extends LinkedHashMap {
    public String value
    public String op
    public String not
    public String ic

    OperationInputType(Map m) {
        this.value = m.value as String
        this.op = m.op as String
        this.not = m.not as String
        this.ic = m.ic as String

        if (value != null) this.put("value", value)
        if (op != null) this.put("op", op)
        if (not != null) this.put("not", not)
        if (ic != null) this.put("ic", ic)
    }

    OperationInputType (String value, String op, String not, String ic) {
        this([value: value, op: op, not: not, ic: ic])
    }
}
