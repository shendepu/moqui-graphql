package com.moqui.graphql

class DateRangeInputType {
    public String period
    public String poffset
    public String from
    public String thru

    DateRangeInputType(Map m) {
        this.period = m.period
        this.poffset = m.poffset
        this.from = m.from
        this.thru = m.thru
    }

    DateRangeInputType(String period, String poffset, String from, String thru) {
        this.period = period
        this.poffset = poffset
        this.from = from
        this.thru = thru
    }
}
