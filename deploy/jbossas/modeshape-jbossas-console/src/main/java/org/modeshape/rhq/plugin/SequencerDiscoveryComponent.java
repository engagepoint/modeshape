/*
 * ModeShape (http://www.modeshape.org)
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * See the AUTHORS.txt file in the distribution for a full listing of 
 * individual contributors.
 *
 * ModeShape is free software. Unless otherwise indicated, all code in ModeShape
 * is licensed to you under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 * 
 * ModeShape is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.modeshape.rhq.plugin;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.managed.api.ComponentType;
import org.jboss.managed.api.ManagedComponent;
import org.jboss.metatype.api.values.MetaValue;
import org.modeshape.jboss.managed.ManagedRepository;
import org.modeshape.jboss.managed.ManagedSequencerConfig;
import org.modeshape.rhq.plugin.util.ModeShapeManagementView;
import org.modeshape.rhq.plugin.util.PluginConstants;
import org.modeshape.rhq.plugin.util.ProfileServiceUtil;
import org.rhq.core.pluginapi.inventory.DiscoveredResourceDetails;
import org.rhq.core.pluginapi.inventory.InvalidPluginConfigurationException;
import org.rhq.core.pluginapi.inventory.ResourceDiscoveryComponent;
import org.rhq.core.pluginapi.inventory.ResourceDiscoveryContext;

/**
 * 
 */
public class SequencerDiscoveryComponent implements
		ResourceDiscoveryComponent<SequencerComponent> {

	private final Log log = LogFactory
			.getLog(PluginConstants.DEFAULT_LOGGER_CATEGORY);

	/**
	 * {@inheritDoc}
	 * 
	 * @see org.rhq.core.pluginapi.inventory.ResourceDiscoveryComponent#discoverResources(org.rhq.core.pluginapi.inventory.ResourceDiscoveryContext)
	 */
	public Set<DiscoveredResourceDetails> discoverResources(
			ResourceDiscoveryContext discoveryContext)
			throws InvalidPluginConfigurationException, Exception {

		Set<DiscoveredResourceDetails> discoveredResources = new HashSet<DiscoveredResourceDetails>();

		ManagedComponent mc = ProfileServiceUtil
				.getManagedComponent(
						((EngineComponent) discoveryContext
								.getParentResourceComponent()).getConnection(),
						new ComponentType(
								PluginConstants.ComponentType.SequencingService.MODESHAPE_TYPE,
								PluginConstants.ComponentType.SequencingService.MODESHAPE_SUB_TYPE),
						PluginConstants.ComponentType.SequencingService.NAME);

		if (mc == null) {
			return discoveredResources;
		}

		String operation = "getSequencers";

		MetaValue sequencers = ModeShapeManagementView.executeManagedOperation(
				mc, operation, new MetaValue[] { null });

		if (sequencers == null) {
			return discoveredResources;
		}

		Collection<ManagedSequencerConfig> sequencerCollection = ModeShapeManagementView
				.getSequencerCollectionValue(sequencers);

		for (ManagedSequencerConfig managedSequencer : sequencerCollection) {

			String name = managedSequencer.getName();

			/**
			 * 
			 * A discovered resource must have a unique key, that must stay the
			 * same when the resource is discovered the next time
			 */
			DiscoveredResourceDetails detail = new DiscoveredResourceDetails(
					discoveryContext.getResourceType(), // ResourceType
					name, // Resource Key
					name, // Resource name
					null, // version
					managedSequencer.getDescription(), // Description
					discoveryContext.getDefaultPluginConfiguration(), // Plugin
																		// config
					null // Process info from a process scan
			);

			// Add to return values
			discoveredResources.add(detail);
			log.info("Discovered ModeShape Sequencer: " + name);
		}

		return discoveredResources;

	}
}