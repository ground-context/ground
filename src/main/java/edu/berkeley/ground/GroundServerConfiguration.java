package edu.berkeley.ground;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import io.dropwizard.elasticsearch.config.EsConfiguration;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.NotNull;

public class GroundServerConfiguration extends Configuration {
    @NotEmpty
    private String dbType;

    @NotEmpty
    private String dbName;

    @NotEmpty
    private String dbHost;

    @NotNull
    private Integer dbPort;

    @NotEmpty
    private String dbUser;

    @NotEmpty
    private String dbPassword;

    @NotNull
    private EsConfiguration esConfiguration;

    @JsonProperty
    public String getDbType() {
        return this.dbType;
    }

    @JsonProperty
    public void setDbType(String dbType) {
        this.dbType = dbType;
    }

    @JsonProperty
    public String getDbName() {
        return this.dbName;
    }

    @JsonProperty
    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    @JsonProperty
    public String getDbHost() {
        return this.dbHost;
    }

    @JsonProperty
    public void setDbHost(String dbHost) {
        this.dbHost = dbHost;
    }

    @JsonProperty
    public Integer getDbPort() {
        return this.dbPort;
    }

    @JsonProperty
    public void setDbPort(Integer dbPort) {
        this.dbPort = dbPort;
    }

    @JsonProperty
    public String getDbUser() {
        return this.dbUser;
    }

    @JsonProperty
    public void setDbUser(String dbUser) {
        this.dbUser = dbUser;
    }

    @JsonProperty
    public String getDbPassword() {
        return this.dbPassword;
    }

    @JsonProperty
    public void setDbPassword(String dbPassword) {
        this.dbPassword = dbPassword;
    }

    @JsonProperty
    public EsConfiguration getEsConfiguration() {
        return this.esConfiguration;
    }

    @JsonProperty
    public void setEsConfiguration(EsConfiguration esConfiguration) {
        this.esConfiguration = esConfiguration;
    }
}