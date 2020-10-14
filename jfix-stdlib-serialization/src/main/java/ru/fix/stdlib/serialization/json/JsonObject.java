package ru.fix.stdlib.serialization.json;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.HashMap;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class JsonObject {

    /**
     * Other fields which processor will ignore
     */
    @JsonIgnore
    private final Map<String, Object> implicitFields = new HashMap<>();

    @JsonAnyGetter
    public Map<String, Object> otherFields() {
        return implicitFields;
    }

    @JsonAnySetter
    public void setOtherField(String name, Object value) {
        implicitFields.put(name, value);
    }
}

