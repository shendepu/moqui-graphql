# This is Moqui GraphQL Add-On Component

This [Moqui](https://github.com/moqui/moqui-framework) add-on component adds support of [GraphQL](graphql.org) to Moqui. 

The way to use it is just simliar to REST Api in moqui-framework. 

- The GraphQL endpoint is `/graphql/v1?query={graphQLQueryString}` or `/graphql/v1?query={graphQLQueryString}&&variables={graphQLVariables}` 
- The configuration of GraphQL Schema is *.graphql.xml under service directory of component
- Only one schema is produced. Each *.graphql.xml file is produced as a field of root query or mutation with name of schema.@name   

You may try the [demo](https://github.com/shendepu/moqui-graphql-demo)


# References

- [Relay Cursor Connection Specification](https://facebook.github.io/relay/graphql/connections.htm)
- [Relay Input Object Mutations Specification](https://facebook.github.io/relay/graphql/mutations.htm)

# License

Moqui GraphQL is [CC0-licensed](./LICENSE.md). we also provide an addition [copyright and patent grant](./AUTHORS) 