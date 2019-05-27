package io.snello.service.rs;

import io.snello.service.ApiService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Null;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.util.Map;

import static io.snello.management.AppConstants.*;
import static javax.ws.rs.core.Response.ok;

@Path(METADATA_PATH)
@ApplicationScoped
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class MetadataController {

    @Inject
    ApiService apiService;

    static String table = METADATAS;
    static String default_sort = " table_name asc ";


    Logger logger = LoggerFactory.getLogger(getClass());

    @Context
    UriInfo uriInfo;

    @GET
    public Response list(
            @Null @QueryParam(SORT_PARAM) String sort,
            @Null @QueryParam(LIMIT_PARAM) String limit,
            @Null @QueryParam(START_PARAM) String start) throws Exception {
        if (sort != null)
            logger.info(SORT_DOT_DOT + sort);
        else
            sort = default_sort;
        if (limit != null)
            logger.info(LIMIT_DOT_DOT + limit);
        if (start != null)
            logger.info(START_DOT_DOT + start);
        Integer l = limit == null ? 10 : Integer.valueOf(limit);
        Integer s = start == null ? 0 : Integer.valueOf(start);
        MultivaluedMap<String, String> mappa = uriInfo.getQueryParameters();
        return ok(apiService.list(table, uriInfo.getQueryParameters(), sort, l, s))
                .header(SIZE_HEADER_PARAM, EMPTY + apiService.count(table, uriInfo.getQueryParameters()))
                .header(TOTAL_COUNT_HEADER_PARAM, EMPTY + apiService.count(table, uriInfo.getQueryParameters())).build();
    }


    @Path(UUID_PATH_PARAM)
    @GET
    public Response fetch(@NotNull String uuid) throws Exception {
        return ok(apiService.fetch(null, table, uuid, UUID)).build();
    }


    @POST
    public Response post(Map<String, Object> map) throws Exception {
        if (map.get(TABLE_NAME) == null) {
            throw new Exception(MSG_TABLE_NAME_IS_EMPTY);
        }
        map.put(UUID, java.util.UUID.randomUUID().toString());
        map = apiService.create(table, map, UUID);
        return ok(map).build();
    }

    @Path(UUID_PATH_PARAM)
    @PUT
    public Response put(Map<String, Object> map, @NotNull String uuid) throws Exception {
        String key = "uuid";
        map = apiService.merge(table, map, uuid, key);
        return ok(map).build();
    }

    @DELETE
    @Path(UUID_PATH_PARAM)
    public Response delete(@NotNull String uuid) throws Exception {
        boolean result = apiService.delete(table, uuid, UUID);
        if (result) {
            return ok().build();
        }
        throw new WebApplicationException();
    }
}