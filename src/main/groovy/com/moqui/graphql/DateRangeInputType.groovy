package com.moqui.graphql

import groovy.transform.CompileStatic

@CompileStatic
class DateRangeInputType extends LinkedHashMap {
    public String period
    public String poffset
    public String from
    public String thru

    DateRangeInputType(Map m) {
        this.period = m.period as String
        this.poffset = m.poffset as String
        this.from = m.from as String
        this.thru = m.thru as String
        if (this.period != null) this.put("period", this.period)
        if (this.poffset != null) this.put("poffset", this.poffset)
        if (this.from != null) this.put("from", this.from)
        if (this.thru != null) this.put("thru", this.thru)
    }

    DateRangeInputType(String period, String poffset, String from, String thru) {
        this([period: period, poffset: poffset, from: from, thru: thru])
    }
}
