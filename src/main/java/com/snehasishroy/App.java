package com.snehasishroy;

import com.snehasishroy.module.GuiceModule;
import com.snehasishroy.resources.Job;
import com.snehasishroy.resources.Worker;
import io.dropwizard.core.Application;
import io.dropwizard.core.setup.Bootstrap;
import io.dropwizard.core.setup.Environment;
import io.federecio.dropwizard.swagger.SwaggerBundle;
import io.federecio.dropwizard.swagger.SwaggerBundleConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import ru.vyarus.dropwizard.guice.GuiceBundle;

@Slf4j
public class App extends Application<AppConfiguration> {

    public static void main(String[] args) throws Exception {
        new App().run(args);
    }

    @Override
    public void initialize(Bootstrap<AppConfiguration> bootstrap) {
        bootstrap.addBundle(guiceBundle());
        bootstrap.addBundle(new SwaggerBundle<>() {
            @Override
            protected SwaggerBundleConfiguration getSwaggerBundleConfiguration(AppConfiguration appConfiguration) {
                return appConfiguration.getSwaggerBundleConfiguration();
            }
        });
    }

    @Override
    public void run(AppConfiguration c, Environment e) {
        log.info("Registering REST resources");
        e.jersey().register(Worker.class);
        e.jersey().register(Job.class);
    }

    private GuiceModule createGuiceModule() {
        return new GuiceModule();
    }

    private GuiceBundle guiceBundle() {
        return GuiceBundle.builder()
                .enableAutoConfig()
                .modules(new GuiceModule())
                .build();
    }
}
