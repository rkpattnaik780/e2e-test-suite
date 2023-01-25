package io.managed.services.test.k8.managedkafka.v1alpha1;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Buildable(
        builderPackage = "io.fabric8.kubernetes.api.builder",
        editableEnabled = false)
@ToString
@EqualsAndHashCode
@JsonInclude(JsonInclude.Include.NON_NULL)
@Getter
@Setter
public class Profile {

    Integer maxNodes;

}
