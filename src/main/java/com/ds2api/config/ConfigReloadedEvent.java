package com.ds2api.config;

import org.springframework.context.ApplicationEvent;

/**
 * Published after a successful config hot-reload via /admin/config POST.
 * Listeners (AccountPoolManager, etc.) rebuild their state from the updated config.
 */
public class ConfigReloadedEvent extends ApplicationEvent {
    private final Ds2Config updatedConfig;

    public ConfigReloadedEvent(Object source, Ds2Config updatedConfig) {
        super(source);
        this.updatedConfig = updatedConfig;
    }

    public Ds2Config getUpdatedConfig() {
        return updatedConfig;
    }
}
