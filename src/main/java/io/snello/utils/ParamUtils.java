package io.snello.utils;


import javax.ws.rs.core.MultivaluedMap;
import java.util.List;
import java.util.Map;

public class ParamUtils {

    static final String AND = " AND ";
    static final String EQU = "=";
    static final String NE = "_ne";
    static final String _NE = "!=";
    static final String LT = "_lt";
    static final String _LT = "<";
    static final String GT = "_gt";
    static final String _GT = ">";
    static final String LTE = "_lte";
    static final String _LTE = "<=";
    static final String GTE = "_gte";
    static final String _GTE = ">=";
    static final String CNT = "_contains";
    static final String _CNT = " LIKE ";
    static final String _LIKE = "%";
    static final String CONTSS = "_containss";
    static final String NCNT = "_ncontains";
    static final String _NCNT = " NOT LIKE ";
    static final String SPACE = " ";

    // _limit=2 _start=1 _sort=page_title:desc
    static final String _LIMIT = "_limit";
    static final String _START = "_start";
    static final String _SORT = "_sort";
    static final String _SELECT_FIELDS = "select_fields";


    public static String select_fields(MultivaluedMap<String, String> httpParameters) {
        if (httpParameters == null || httpParameters.isEmpty()) {
            return null;
        }
        if (httpParameters.containsKey("select_fields") && httpParameters.getFirst("select_fields") != null && !httpParameters.getFirst("select_fields").trim().isEmpty()) {
            return httpParameters.getFirst("select_fields");
        }
        return null;
    }

    public static void where(MultivaluedMap<String, String> httpParameters, StringBuffer where, List<Object> in) {
        if (httpParameters == null || httpParameters.isEmpty()) {
            return;
        }
        /*
            =: Equals
            _ne: Not equals
            _lt: Lower than
            _gt: Greater than
            _lte: Lower than or equal to
            _gte: Greater than or equal to
            _contains: Contains
            _containss: Contains case sensitive
         */
        for (String key : httpParameters.keySet()) {
            String value;
            List<String> key_value = httpParameters.get(key);
            if (key.equals(_LIMIT) || key.equals(_START) || key.equals(_SORT) || key.equals(_SELECT_FIELDS)) {
                continue;
            }
            if (key_value != null && key_value.size() > 0 && key_value.get(0) != null
                    && !key_value.get(0).trim().isEmpty()) {
                value = key_value.get(0);
            } else {
                continue;
            }

            if (key.endsWith(NE)) {
                if (where.length() > 0) {
                    where.append(AND);
                }
                where.append(key.substring(0, key.length() - NE.length()));
                where.append(_NE);
                where.append(" ? ").append(SPACE);
                in.add(value);
                continue;
            }
            if (key.endsWith(LT)) {
                if (where.length() > 0) {
                    where.append(AND);
                }
                where.append(key.substring(0, key.length() - LT.length()));
                where.append(_LT);
                where.append(" ? ").append(SPACE);
                in.add(value);
                continue;
            }
            if (key.endsWith(GT)) {
                if (where.length() > 0) {
                    where.append(AND);
                }
                where.append(key.substring(0, key.length() - GT.length()));
                where.append(_GT);
                where.append(" ? ").append(SPACE);
                in.add(value);
                continue;
            }
            if (key.endsWith(LTE)) {
                if (where.length() > 0) {
                    where.append(AND);
                }
                where.append(key.substring(0, key.length() - LTE.length()));
                where.append(_LTE);
                where.append(" ? ").append(SPACE);
                in.add(value);
                continue;
            }

            if (key.endsWith(GTE)) {
                if (where.length() > 0) {
                    where.append(AND);
                }
                where.append(key.substring(0, key.length() - GTE.length()));
                where.append(_GT);
                where.append(" ? ").append(SPACE);
                in.add(value);
                continue;
            }
            if (key.endsWith(CNT)) {
                if (where.length() > 0) {
                    where.append(AND);
                }
                where.append(key.substring(0, key.length() - CNT.length()));
                where.append(_CNT);
                where.append(" ? ").append(SPACE);
                in.add(_LIKE + value + _LIKE);
                continue;
            }
            if (key.endsWith(CONTSS)) {
                if (where.length() > 0) {
                    where.append(AND);
                }
                where.append(" lower( " + key.substring(0, key.length() - CONTSS.length()) + " ) ");
                where.append(_CNT);
                where.append(" ? ").append(SPACE);
                in.add(_LIKE + value.toLowerCase() + _LIKE);
                continue;
            }
            if (key.endsWith(NCNT)) {
                if (where.length() > 0) {
                    where.append(AND);
                }
                where.append(key);
                where.append(_NCNT);
                where.append(" ? ").append(SPACE);
                in.add(_LIKE + value.toLowerCase() + _LIKE);
                continue;
            }
            if (where.length() > 0) {
                where.append(AND);
            }
            where.append(key).append(EQU).append(" ? ").append(SPACE);
            in.add(value);
        }
    }
}
