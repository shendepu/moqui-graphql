package com.moqui.graphql

class PaginationInputType {
    public int pageIndex
    public int pageSize
    public boolean pageNoLimit
    public String orderByField
    public int first
    public String after
    public int last
    public String before

    PaginationInputType(Map m) {
        this.pageIndex = m.pageIndex
        this.pageSize = m.pageSize
        this.pageNoLimit = m.pageNoLimit
        this.orderByField = m.orderByField
        this.first = m.first
        this.after = m.after
        this.last = m.last
        this.before = m.before
    }

    PaginationInputType(int pageIndex, int pageSize, boolean pageNoLimit, String orderByField) {
        this.pageIndex = pageIndex
        this.pageSize = pageSize
        this.pageNoLimit = pageNoLimit
        this.orderByField = orderByField
    }

    PaginationInputType(int first, String after, int last, String before) {
        this.first = first
        this.after = after
        this.last = last
        this.before = before
    }
}
