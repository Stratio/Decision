/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.streaming.extensions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.core.event.StreamEvent;
import org.wso2.siddhi.core.event.in.InEvent;
import org.wso2.siddhi.core.event.in.InListEvent;
import org.wso2.siddhi.core.query.QueryPostProcessingElement;
import org.wso2.siddhi.core.query.processor.window.WindowProcessor;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.expression.Variable;
import org.wso2.siddhi.query.api.extension.annotation.SiddhiExtension;

@SiddhiExtension(namespace = "stratio", function = "distinct")
public class DistinctWindowExtension extends WindowProcessor {

    private Variable variable;
    private List<Variable> constants;
    private Map<String, Object> lastObjectHash;

    @Override
    /**
     *This method called when processing an event
     */
    protected void processEvent(InEvent inEvent) {
        acquireLock();
        try {
            doProcessing(inEvent);
        } finally

        {
            releaseLock();
        }

    }

    @Override
    /**
     *This method called when processing an event list
     */
    protected void processEvent(InListEvent inListEvent) {

        for (int i = 0; i < inListEvent.getActiveEvents(); i++) {
            InEvent inEvent = (InEvent) inListEvent.getEvent(i);
            processEvent(inEvent);
        }
    }

    @Override
    /**
     * This method iterate through the events which are in window
     */
    public Iterator<StreamEvent> iterator() {
        return null;
    }

    @Override
    /**
     * This method iterate through the events which are in window but used in distributed processing
     */
    public Iterator<StreamEvent> iterator(String s) {
        return null;
    }

    @Override
    /**
     * This method used to return the current state of the window, Used for persistence of data
     */
    protected Object[] currentState() {
        return new Object[] { variable, constants, lastObjectHash };
    }

    /**
     * This method is used to restore from the persisted state
     */
    @Override
    @SuppressWarnings("unchecked")
    protected void restoreState(Object[] objects) {
        if (objects != null && objects.length != 0) {
            variable = (Variable) objects[0];
            constants = (List<Variable>) objects[1];
            lastObjectHash = (Map<String, Object>) objects[2];
        }
    }

    @Override
    /**
     * Method called when initialising the extension
     */
    protected void init(Expression[] expressions, QueryPostProcessingElement queryPostProcessingElement,
            AbstractDefinition abstractDefinition, String s, boolean b, SiddhiContext siddhiContext) {
        constants = new ArrayList<>();
        lastObjectHash = new HashMap<>();
        for (int i = 0; i < expressions.length; i++) {
            Variable var = ((Variable) expressions[i]);
            var = Variable.variable(var.getStreamId(), abstractDefinition.getAttributePosition(var.getAttributeName()),
                    var.getAttributeName());
            if (i == 0) {
                variable = var;
            } else {
                constants.add(var);
            }
        }
    }

    private void doProcessing(InEvent event) {
        StringBuilder sb = new StringBuilder();
        for (Variable cons : constants) {
            sb.append(event.getData(cons.getPosition()));
        }
        String key = sb.toString();
        boolean process = false;
        if (!lastObjectHash.containsKey(key) || !lastObjectHash.get(key).equals(event.getData(variable.getPosition()))) {
            lastObjectHash.put(key, event.getData(variable.getPosition()));
            process = true;
        }

        if (process) {
            nextProcessor.process(event);
        }
    }

    @Override
    public void destroy() {
        variable = null;
        constants = null;
        lastObjectHash = null;
    }
}
