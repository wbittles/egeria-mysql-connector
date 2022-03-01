/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.openmetadata.adapters.connectors.integration.mysql;

import org.odpi.openmetadata.frameworks.connectors.ConnectorProviderBase;
import org.odpi.openmetadata.frameworks.connectors.properties.beans.ConnectorType;

public class MysqlDatabaseProvider extends ConnectorProviderBase
{
    static final String  connectorTypeGUID = "f2283203-eee9-4243-b7fd-17cd10b79931";
    static final String  connectorTypeName = "Mysql Database Connector";
    static final String  connectorTypeDescription = "Connector supports mysql Database instance";
   /**
     * Constructor used to initialize the ConnectorProviderBase with the Java class name of the specific
     * store implementation.
     */
    public MysqlDatabaseProvider()
    {
        Class<?> connectorClass = MysqlDatabaseConnector.class;

        super.setConnectorClassName("org.odpi.openmetadata.adapters.connectors.integration.mysql.MysqlDatabaseConnector");

        ConnectorType connectorType = new ConnectorType();
        connectorType.setType(ConnectorType.getConnectorTypeType());
        connectorType.setGUID(connectorTypeGUID);
        connectorType.setQualifiedName(connectorTypeName);
        connectorType.setDisplayName(connectorTypeName);
        connectorType.setDescription(connectorTypeDescription);
        connectorType.setConnectorProviderClassName(this.getClass().getName());

        super.connectorTypeBean = connectorType;
    }
}
