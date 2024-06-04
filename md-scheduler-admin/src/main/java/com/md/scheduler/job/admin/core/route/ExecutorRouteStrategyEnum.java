package com.md.scheduler.job.admin.core.route;

import com.md.scheduler.job.admin.core.route.impl.ExecutorRouterFirst;

public enum ExecutorRouteStrategyEnum {
    FIRST("jobconf_route_first", new ExecutorRouterFirst());


    private String title;

    private ExecutorRouter executorRouter;

    ExecutorRouteStrategyEnum(String title, ExecutorRouter router) {
        this.title = title;
        this.executorRouter = router;
    }

    public String getTitle() {
        return title;
    }

    public ExecutorRouter getExecutorRouter() {
        return executorRouter;
    }

    public static ExecutorRouteStrategyEnum match(String name,
                                                  ExecutorRouteStrategyEnum defaultItem) {
        if (name != null) {
            for (ExecutorRouteStrategyEnum item : ExecutorRouteStrategyEnum.values()) {
                if (item.name().equals(name)) {
                    return item;
                }
            }
        }
        return defaultItem;
    }
}

