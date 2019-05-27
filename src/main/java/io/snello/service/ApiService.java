package io.snello.service;

import io.snello.model.Condition;
import io.snello.repository.JdbcRepository;
import io.snello.utils.ParamUtils;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.core.MultivaluedMap;
import java.util.List;
import java.util.Map;

@Singleton
public class ApiService {

    @Inject
    JdbcRepository jdbcRepository;

    public ApiService() {
    }


    public long count(String table, MultivaluedMap<String, String> httpParameters) throws Exception {
        String alias_condition = null;
        List<Condition> conditions = null;
        return jdbcRepository.count(table, alias_condition, httpParameters, conditions);
    }

    public boolean exist(String table, String table_key, Object uuid) throws Exception {
        return jdbcRepository.exist(table, table_key, uuid);
    }


    public List<Map<String, Object>> list(String table, MultivaluedMap<String, String> httpParameters, String sort, int limit, int start) throws Exception {
        String select_fields = ParamUtils.select_fields(httpParameters);
        String alias_condition = null;
        List<Condition> conditions = null;
        return jdbcRepository.list(table, select_fields, alias_condition, httpParameters, conditions, sort, limit, start);
    }

    public Map<String, Object> create(String table, Map<String, Object> map, String table_key) throws Exception {
        table = initTable(table);
        table_key = initTableKey(table, table_key);
        return jdbcRepository.create(table, table_key, map);
    }

    public Map<String, Object> merge(String table, Map<String, Object> map, String key, String table_key) throws Exception {
        table = initTable(table);
        table_key = initTableKey(table, table_key);
        return jdbcRepository.update(table, table_key, map, key);
    }


    public Map<String, Object> createIfNotExists(String table, Map<String, Object> map, String table_key) throws Exception {
        return jdbcRepository.create(table, table_key, map);
    }

    public Map<String, Object> mergeIfNotExists(String table, Map<String, Object> map, String key, String table_key) throws Exception {
        return jdbcRepository.update(table, table_key, map, key);
    }


    public String initTable(String table) throws Exception {
        return table;
    }


    public String initTableKey(String table, String table_key) throws Exception {
        return table_key;
    }


    public Map<String, Object> fetch(MultivaluedMap<String, String> httpParameters, String table, String uuid, String table_key) throws Exception {
        String select_fields = ParamUtils.select_fields(httpParameters);
        return jdbcRepository.fetch(select_fields, table, table_key, uuid);
    }

    public boolean delete(String table, String uuid, String table_key) throws Exception {
        return jdbcRepository.delete(table, table_key, uuid);
    }

    public void batch(String[] queries) throws Exception {
        jdbcRepository.batch(queries);
    }

    public boolean query(String query, List<Object> values) throws Exception {
        return jdbcRepository.query(query, values);
    }

}
