package io.managed.services.test.k8.managedkafka.v1alpha1;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import javax.validation.constraints.NotNull;

import java.util.LinkedHashMap;
import java.util.Map;

@Buildable(
        builderPackage = "io.fabric8.kubernetes.api.builder",
        editableEnabled = false
)
@ToString
@EqualsAndHashCode
@JsonInclude(Include.NON_NULL)
@Getter
@Setter
public class ManagedKafkaAgentSpec {
    @NotNull
    ObservabilityConfiguration observability;
    Map<String, Profile> capacity = new LinkedHashMap<>();
    NetworkConfiguration net;
}
