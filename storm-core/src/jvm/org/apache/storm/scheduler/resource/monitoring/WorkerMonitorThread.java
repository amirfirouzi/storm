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

import java.util.Map;

public class WorkerMonitorThread extends Thread {
	
	public void run(Map conf) {
		
		while (true) {
			try {
				Thread.sleep(MonitorConfiguration.getInstance(conf).getTimeWindowSlotLength() * 1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			try {
				WorkerMonitor.getInstance(conf).sampleStats();
				WorkerMonitor.getInstance(conf).storeStats();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
