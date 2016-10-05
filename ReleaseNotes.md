# Moqui GraphQL Add-On Release Notes
 
## Release 0.0.1 - TBD
 
New Features:

- Configuration of component/service/*.graphql.xml to define GraphQL Schema which utilizing [GraphQL-Java](https://github.com/graphql-java/graphql-java)
- Added Timestamp GraphQLScalarType
- *.graphql.xml can be hot reloaded by configuring expire-time-idle of cache service.graphql.schema.definition and service.graphql.graphql


 
## Long Term To Do List - aka Informal Road Map

- Auto add all of entity as GraphQLObjectType
- Extend Object-Type to add field 
- Extend Field by adding data-fetcher
- Add entity method for data-fetcher
- Add test cases



