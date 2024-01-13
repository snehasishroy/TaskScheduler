package com.snehasishroy;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.core.Configuration;
import io.federecio.dropwizard.swagger.SwaggerBundleConfiguration;
import lombok.*;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class AppConfiguration extends Configuration {

    @JsonProperty("swagger")
    SwaggerBundleConfiguration swaggerBundleConfiguration;
}
