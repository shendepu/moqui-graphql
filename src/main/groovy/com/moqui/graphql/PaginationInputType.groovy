package com.moqui.graphql

class PaginationInputType {
    public Integer pageIndex
    public Integer pageSize
    public Boolean pageNoLimit
    public String orderByField
    public Integer first
    public String after
    public Integer last
    public String before
    public String type  // 'offset' or 'cursor-after' or 'cursor-before'

    PaginationInputType(Map m) {
        this.pageIndex = m.pageIndex
        this.pageSize = m.pageSize
        this.pageNoLimit = m.pageNoLimit
        this.orderByField = m.orderByField
        this.first = m.first
        this.after = m.after
        this.last = m.last
        this.before = m.before
        this.type = m.type ?: 'offset'
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

    @Override
    String toString() {
        switch (type) {
            case 'offset':
                return "[type: ${type}, pageIndex: ${pageIndex}, pageSize: ${pageSize}, pageNoLimit: ${pageNoLimit}, orderByField: ${orderByField}]"
            case "cursor-after":
                return "[type: ${type}, after: ${after}, first: ${first}, pageNoLimit: ${pageNoLimit}, orderByField: ${orderByField}]"
            case "cursor-before":
                return "[type: ${type}, before: ${before}, last: ${last}, pageNoLimit: ${pageNoLimit}, orderByField: ${orderByField}]"
        }
    }
}
