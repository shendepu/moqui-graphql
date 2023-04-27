# Fork of the Moqui GraphQL Add-On Component
Reasons for fork ([See Original Project](shendepu/moqui-graphql)):
- At time of writing, last commit was over a year ago and has not been actively been developed since 2018.
- Breaking changes have been introduced because of JCenter sunsetting [see PR](https://github.com/shendepu/moqui-graphql/pull/8)



This [Moqui](https://github.com/moqui/moqui-framework) add-on component adds support of [GraphQL](graphql.org) to Moqui. 

The way to use it is just similar to REST Api in moqui-framework. 

- The GraphQL endpoint is `/graphql/v1?query={graphQLQueryString}` or `/graphql/v1?query={graphQLQueryString}&&variables={graphQLVariables}` 
- The configuration of GraphQL Schema is *.graphql.xml under service directory of component
- Only one schema is produced. Each *.graphql.xml file is produced as a field of root query or mutation with name of schema.@name
- GraphQL execution strategy is BatchedExecutionStrategy
- entity-fetcher is implemented with EntityBatchedDataFetcher

You may try the [demo](https://github.com/shendepu/moqui-graphql-demo)

## About EntityBatchedDataFetcher

In following cases it combined entity-find (sql query essentially) on same field into one entity-find
- one operation: it turn multiple entity-find.one() into entity-find by adding in condition if there is one relationship key map or by adding a tuple (all relationship key map) or condition
- list operation: when there is no pagination argument or pageInfo in field selection set
 
In following cases it iterate over environment source to run entity-find, so multiple times entity-find is executed, slow.
- list operation: when there is pagination argument or pageInfo in field selection set. this can't be done combining them since each list need count to calculate pageInfo and impossible to equally get same number result for each source in one entity-find.

One special case:
- list operation: when there is no pagination argument or pageInfo, but no relationship key map, a pagination limit is enforced to avoid return all data. Due to there should be just one item in environment source, so iterate over it to execute entity-find is fine for performance. 

# References

- [Relay Cursor Connection Specification](https://facebook.github.io/relay/graphql/connections.htm)
- [Relay Input Object Mutations Specification](https://facebook.github.io/relay/graphql/mutations.htm)

# License

Moqui GraphQL is [CC0-licensed](./LICENSE.md). we also provide an addition [copyright and patent grant](./AUTHORS) 
