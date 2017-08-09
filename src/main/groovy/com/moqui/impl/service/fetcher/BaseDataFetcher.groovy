package com.moqui.impl.service.fetcher

import com.moqui.graphql.DataFetchingException
import graphql.schema.DataFetcher
import graphql.schema.DataFetchingEnvironment
import groovy.transform.CompileStatic
import org.moqui.BaseException
import org.moqui.context.ArtifactAuthorizationException
import org.moqui.context.ArtifactTarpitException
import org.moqui.context.AuthenticationRequiredException
import org.moqui.context.ExecutionContextFactory
import org.moqui.impl.webapp.ScreenResourceNotFoundException


import static com.moqui.impl.service.GraphQLSchemaDefinition.FieldDefinition

@CompileStatic
abstract class BaseDataFetcher implements DataFetcher {
    @SuppressWarnings("GrFinalVariableAccess")
    protected final ExecutionContextFactory ecf
    @SuppressWarnings("GrFinalVariableAccess")
    protected final FieldDefinition fieldDef

    BaseDataFetcher(FieldDefinition fieldDef, ExecutionContextFactory ecf) {
        this.ecf = ecf
        this.fieldDef = fieldDef
    }

    @Override
    Object get(DataFetchingEnvironment environment) {
        try {
            Object result = fetch(environment)

            if (ecf != null && ecf.getExecutionContext().getMessage().hasError()) {
                throw new DataFetchingException('500', ecf.getExecutionContext().getMessage().getErrorsString())
            }
            return result
        } catch (AuthenticationRequiredException e) {
            throw new DataFetchingException('401', e.getMessage())
        } catch (ArtifactAuthorizationException e) {
            if (!ecf.getExecutionContext().getUser().userId)
                throw new DataFetchingException('401', e.getMessage())

            throw new DataFetchingException('403', e.getMessage())
        } catch (ScreenResourceNotFoundException e) {
            throw new DataFetchingException('404', e.getMessage())
        } catch (ArtifactTarpitException e) {
            throw new DataFetchingException('429', e.getMessage())
        } catch (BaseException e) {
            throw e
        } catch (DataFetchingException e) {
            throw e
        } catch (Throwable t) {
            throw new DataFetchingException("UNKNOWN", t.getMessage() + "\n" + t.getStackTrace().toString())
        } finally {
            if (ecf != null) ecf.getExecutionContext().getMessage().clearErrors()
        }
    }

    Object fetch(DataFetchingEnvironment environment) { return null }
}