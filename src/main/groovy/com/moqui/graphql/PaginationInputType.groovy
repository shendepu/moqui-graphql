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

    PaginationInputType() {}
}
