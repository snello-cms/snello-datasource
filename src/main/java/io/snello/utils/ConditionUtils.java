package io.snello.utils;

import io.snello.model.Condition;

import javax.ws.rs.core.MultivaluedMap;
import java.util.List;

public class ConditionUtils {


    public static void where(MultivaluedMap<String, String> httpParameters, List<Condition> conditions, StringBuffer where, List<Object> in) {
        if (httpParameters == null || httpParameters.isEmpty()) {
            System.out.println("no parameters");
            return;
        }
        if (conditions == null || conditions.isEmpty()) {
            System.out.println("no conditions");
            return;
        }
        for (Condition condition : conditions) {

        }
    }
}
