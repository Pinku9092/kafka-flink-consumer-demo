package com.pinku;

import com.pinku.pojos.Employee;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class EmployeeDeserializationSchema implements DeserializationSchema<Employee> {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Employee deserialize(byte[] bytes) throws IOException {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        return objectMapper.readValue(bytes, Employee.class);
    }

    @Override
    public boolean isEndOfStream(Employee nextElement) {
        return false;
    }

   @Override
    public TypeInformation<Employee> getProducedType() {
        return TypeInformation.of(Employee.class);
    }

}
