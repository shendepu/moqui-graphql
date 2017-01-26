package com.moqui.graphql

class OperationInputType {
    public String value
    public String op
    public String not
    public String ic

    OperationInputType(Map m) {
        this.value = m.value
        this.op = m.op
        this.not = m.not
        this.ic = m.ic
    }

    OperationInputType (String value, String op, String not, String ic) {
        this.value = value
        this.op = op
        this.not = not
        this.ic = ic
    }
}
