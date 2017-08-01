/*******************************************************************************
 * Copyright (c) 2013 Leonardo Aniello, Roberto Baldoni, Leonardo Querzoni.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 * Leonardo Aniello, Roberto Baldoni, Leonardo Querzoni
 *******************************************************************************/
package org.apache.storm.scheduler.resource.monitoring;

import org.apache.log4j.Logger;

import java.util.Map;
import java.util.Properties;

public class MonitorConfiguration {

    private static MonitorConfiguration instance = null;

    private int timeWindowSlotCount;
    private int timeWindowSlotLength;

    private Logger logger;

    private MonitorConfiguration(Map conf) {
        logger = Logger.getLogger(MonitorConfiguration.class);

        try {
            // load configuration from file
            logger.debug("Loading configuration from file");
//            Properties properties = new Properties();
//      properties.load(new FileInputStream("/home/storm/storm-current/db.ini"));
//      logger.debug("Configuration loaded");

//			timeWindowSlotCount = Integer.parseInt(properties.getProperty("time.window.slot.count"));
            timeWindowSlotCount = Integer.parseInt(conf.get("time.window.slot.count").toString());
            timeWindowSlotLength = Integer.parseInt(conf.get("time.window.slot.length").toString());
//            timeWindowSlotLength = Integer.parseInt(properties.getProperty("time.window.slot.length"));
        } catch (Exception e) {
            logger.error("Error loading MonitorConfiguration configuration from file", e);
        }
    }

    public synchronized static MonitorConfiguration getInstance(Map conf) {
        if (instance == null)
            instance = new MonitorConfiguration(conf);
        return instance;
    }

    public synchronized static MonitorConfiguration getInstance(){
        return instance;
    }

    /*
     * @Return the length of monitoring time window, in seconds
     */
    public int getTimeWindowLength() {
        return timeWindowSlotCount * timeWindowSlotLength;
    }

    public int getTimeWindowSlotLength() {
        return timeWindowSlotLength;
    }

    public int getTimeWindowSlotCount() {
        return timeWindowSlotCount;
    }
}
