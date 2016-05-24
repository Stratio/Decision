package com.stratio.decision.functions;

/**
 * Created by josepablofernandez on 24/05/16.
 */
import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import com.stratio.decision.configuration.BaseConfiguration;
import com.stratio.decision.configuration.ServiceConfiguration;

/**
 * Created by josepablofernandez on 19/05/16.
 */
public class ActionBaseContext implements Serializable {

    private static final long serialVersionUID = -8765413112252293811L;
    private static AnnotationConfigApplicationContext context = null;
    private static ActionBaseContext instance = null;

    protected static Logger log = LoggerFactory.getLogger(ActionBaseContext.class);

    private ActionBaseContext() {
        if (context == null) {
            context = new
                    AnnotationConfigApplicationContext(BaseConfiguration.class);

            log.error("CONTEXTO SPRING INICIALIZADO: {}" + context.toString());
        }
    }

    public static ActionBaseContext getInstance() {

        if (instance == null) {
            instance = new ActionBaseContext();
        }

        return instance;

    }

    public AnnotationConfigApplicationContext getContext() {
        return context;
    }
}
