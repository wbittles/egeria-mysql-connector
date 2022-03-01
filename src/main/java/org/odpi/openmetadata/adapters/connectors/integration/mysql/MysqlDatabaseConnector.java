package org.odpi.openmetadata.adapters.connectors.integration.mysql;

/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */

import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseColumnElement;
import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseElement;
import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseSchemaElement;
import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseTableElement;
import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseViewElement;
import org.odpi.openmetadata.accessservices.datamanager.properties.*;
import org.odpi.openmetadata.adapters.connectors.integration.mysql.ffdc.AlreadyHandledException;
import org.odpi.openmetadata.adapters.connectors.integration.mysql.ffdc.ExceptionHandler;
import org.odpi.openmetadata.adapters.connectors.integration.mysql.ffdc.MysqlConnectorAuditCode;
import org.odpi.openmetadata.adapters.connectors.integration.mysql.ffdc.MysqlConnectorErrorCode;
import org.odpi.openmetadata.adapters.connectors.integration.mysql.mapper.MysqlMapper;
import org.odpi.openmetadata.adapters.connectors.integration.mysql.properties.MysqlColumn;
import org.odpi.openmetadata.adapters.connectors.integration.mysql.properties.MysqlDatabase;
import org.odpi.openmetadata.adapters.connectors.integration.mysql.properties.MysqlForeignKeyLinks;
import org.odpi.openmetadata.adapters.connectors.integration.mysql.properties.MysqlSchema;
import org.odpi.openmetadata.adapters.connectors.integration.mysql.properties.MysqlTable;
import org.odpi.openmetadata.frameworks.connectors.ffdc.ConnectorCheckedException;
import org.odpi.openmetadata.frameworks.connectors.ffdc.InvalidParameterException;
import org.odpi.openmetadata.frameworks.connectors.ffdc.PropertyServerException;
import org.odpi.openmetadata.frameworks.connectors.ffdc.UserNotAuthorizedException;
import org.odpi.openmetadata.integrationservices.database.connector.DatabaseIntegratorConnector;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MysqlDatabaseConnector extends DatabaseIntegratorConnector
{
    final int startFrom = 0;
    final int pageSize = 0;

    @Override
    public void refresh() throws ConnectorCheckedException
    {
        String methodName = "MysqlConnector.refresh";

        MysqlSourceDatabase source = new MysqlSourceDatabase(connectionProperties);
        try
        {
            /*
            get a list of databases currently hosted in Mysql
            and a list of databases already known by Egeria
             */
            List<MysqlDatabase> MysqlDatabases = source.getDabases();
            List<DatabaseElement> egeriaDatabases = getContext().getMyDatabases(startFrom, pageSize);

            /*
            first we remove any Egeria databases that are no longer present in Mysql
             */
            egeriaDatabases = deleteDatabases( MysqlDatabases, egeriaDatabases );

            for (MysqlDatabase MysqlDatabase : MysqlDatabases)
            {
                boolean found = false;
                if (egeriaDatabases == null  )
                {
                    if( MysqlDatabases.size() > 0 )
                    {
                        /*
                    we have no databases in Egeria
                    so all databases are new
                     */
                        addDatabase(MysqlDatabase);
                    }
                }
                else
                {
                    /*
                    check if the database is known to Egeria
                    and needs to be updated
                     */
                    for (DatabaseElement egeriaDatabase : egeriaDatabases)
                    {

                        String egeriaQN =  egeriaDatabase.getDatabaseProperties().getQualifiedName();
                        String MysqlQN = MysqlDatabase.getQualifiedName();

                        if (egeriaQN.equals(MysqlQN))
                        {
                        /*
                        we have found an exact instance to update
                         */
                            found = true;
                            updateDatabase(MysqlDatabase, egeriaDatabase);
                            break;
                        }
                    }
                    /*
                    this is a new database so add it
                     */
                    if (!found)
                    {
                        addDatabase(MysqlDatabase);
                    }
                }
            }
        }
        catch (SQLException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(  methodName,
                                        MysqlConnectorAuditCode.ERROR_READING_MYSQL.getMessageDefinition(methodName,
                                                                                                                error.getClass().getName(),
                                                                                                                error.getMessage()),
                                        error);
            }

            throw new ConnectorCheckedException(MysqlConnectorErrorCode.ERROR_READING_FROM_MYSQL.getMessageDefinition(methodName, error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName, error);

        }
        catch (InvalidParameterException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        MysqlConnectorAuditCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                        error);
            }

            throw new ConnectorCheckedException(MysqlConnectorErrorCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName),
                    this.getClass().getName(),
                    methodName, error);

        }
        catch (UserNotAuthorizedException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        MysqlConnectorAuditCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                        error);
            }

            throw new ConnectorCheckedException(MysqlConnectorErrorCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName),
                    this.getClass().getName(),
                    methodName, error);

        }
        catch (ConnectorCheckedException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        MysqlConnectorAuditCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                        error);
            }

            throw error;
        }
        catch (AlreadyHandledException error)
        {
            throw new ConnectorCheckedException(MysqlConnectorErrorCode.ALREADY_HANDLED_EXCEPTION.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName, error);

        }
        catch (Exception error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        MysqlConnectorAuditCode.UNEXPECTED_ERROR.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                        error);
            }

            throw new ConnectorCheckedException(MysqlConnectorErrorCode.ERROR_READING_FROM_MYSQL.getMessageDefinition(methodName),
                    this.getClass().getName(),
                    methodName, error);

        }

    }


    /**
     * Trawls through a database updating a database where necessary
     *
     * @param MysqlDatabase the bean properties of a Mysql Database
     * @param egeriaDatabase   the Egeria database
     * @throws AlreadyHandledException this exception has already been logged
     */
    private void updateDatabase(MysqlDatabase MysqlDatabase, DatabaseElement egeriaDatabase) throws AlreadyHandledException
    {
        String methodName = "updateDatabase";

        try
        {
            if (egeriaDatabase != null)
            {
                String guid = egeriaDatabase.getElementHeader().getGUID();
                /*
                have the properties of the database entity changed
                 */
                if (!MysqlDatabase.isEquivalent(egeriaDatabase))
                {
                    /*
                    then we need to update the entity properties
                     */
                    DatabaseProperties props = MysqlMapper.getDatabaseProperties(MysqlDatabase);
                    getContext().updateDatabase(guid, props);

                }

                /*
                now trawl through the rest of the schema
                updating where necessary
                 */
                updateSchemas(guid, MysqlDatabase.getName());
            }
        }
        catch (InvalidParameterException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName));

        }
        catch (UserNotAuthorizedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName));

        }
        catch (PropertyServerException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (ConnectorCheckedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (Exception error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.UNEXPECTED_ERROR.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.UNEXPECTED_ERROR.getMessageDefinition(methodName));

        }

    }

    /**
     * iterates over the database schemas updating where necessary
     *
     * @param databaseGUID   the Egeria database
     * @throws AlreadyHandledException this exception has already been logged
     */
    private void updateSchemas(String databaseGUID, String name) throws AlreadyHandledException
    {
        String methodName = "updateSchemas";
        MysqlSourceDatabase source = new MysqlSourceDatabase(this.connectionProperties);

        try
        {
               /*
            get a list of databases schema currently hosted in Mysql
            and remove any databases schemas that have been dropped since the last refresh
             */
            List<MysqlSchema> MysqlSchemas = source.getDatabaseSchema(name);
            List<DatabaseSchemaElement> egeriaSchemas = getContext().getSchemasForDatabase(databaseGUID, startFrom, pageSize);

            if( egeriaSchemas != null )
            {
                egeriaSchemas = deleteSchemas( MysqlSchemas, egeriaSchemas);
            }

            for (MysqlSchema MysqlSchema : MysqlSchemas)
            {
                boolean found = false;
                /*
                we have no schemas in Egeria
                so all schemas are new
                 */
                if (egeriaSchemas == null)
                {
                    if( MysqlSchemas.size() > 0 )
                    {
                        addSchemas(name, databaseGUID);
                    }
                }
                else
                {
                    /*
                    check if the schema is known to Egeria
                    and needs to be updated
                     */
                    for (DatabaseSchemaElement egeriaSchema : egeriaSchemas)
                    {
                        if (egeriaSchema.getDatabaseSchemaProperties().getQualifiedName().equals(MysqlSchema.getQualifiedName()))
                        {
                        /*
                        we have found an exact instance to update
                         */
                            found = true;
                            updateSchema(MysqlSchema, egeriaSchema);
                            break;
                        }
                    }
                    /*
                    this is a new database so add it
                     */
                    if (!found)
                    {
                        addSchema(MysqlSchema, databaseGUID);
                    }
                }
            }
        }
        catch (SQLException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.ERROR_READING_MYSQL.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.ERROR_READING_FROM_MYSQL.getMessageDefinition(methodName));

        }

        catch (ConnectorCheckedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName));

        }

        catch (InvalidParameterException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName));

        }
        catch (UserNotAuthorizedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName));

        }

        catch (PropertyServerException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName));

        }
        catch (Exception error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.UNEXPECTED_ERROR.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.UNEXPECTED_ERROR.getMessageDefinition(methodName));

        }

    }

    /**
     * Changes the properties of an Egeria schema entity
     *
     * @param MysqlSchema            the Mysql Schema properties
     * @param egeriaSchema          the Egeria schema
     * @throws AlreadyHandledException this exception has already been logged
     */
    private void updateSchema( MysqlSchema MysqlSchema, DatabaseSchemaElement egeriaSchema) throws AlreadyHandledException
    {
        String methodName = "updateSchema";
        try
        {
            if ( !MysqlSchema.isEquivalent(egeriaSchema) )
            {
                DatabaseSchemaProperties props = MysqlMapper.getSchemaProperties(MysqlSchema);
                getContext().updateDatabaseSchema(egeriaSchema.getElementHeader().getGUID(), props);
            }
            updateTables(MysqlSchema, egeriaSchema);
            updateViews(MysqlSchema, egeriaSchema);

        }
        catch (InvalidParameterException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (PropertyServerException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName));

        }
        catch (UserNotAuthorizedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (ConnectorCheckedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (Exception error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.UNEXPECTED_ERROR.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.UNEXPECTED_ERROR.getMessageDefinition(methodName));

        }

    }

    /**
     * @param MysqlSchema the Mysql schema bean
     * @param egeriaSchema   the Egeria schema bean
     * @throws AlreadyHandledException this exception has already been logged
     */
    private void updateTables(MysqlSchema MysqlSchema, DatabaseSchemaElement egeriaSchema) throws AlreadyHandledException
    {

        final String methodName = "updateTables";

        String schemaGuid = egeriaSchema.getElementHeader().getGUID();
        MysqlSourceDatabase source = new MysqlSourceDatabase(this.connectionProperties);

        try
        {
            /*
            get a list of databases tables currently hosted in Mysql
            and remove any tables that have been dropped since the last refresh
             */
            List<MysqlTable> MysqlTables = source.getTables(MysqlSchema.getSchema_name());
            List<DatabaseTableElement> egeriaTables = getContext().getTablesForDatabaseSchema(schemaGuid, startFrom, pageSize);

            /*
            remove tables from Egeria that are no longer needed
             */
            egeriaTables = deleteTables( MysqlTables, egeriaTables);

            for (MysqlTable MysqlTable : MysqlTables)
            {
                boolean found = false;
                /*
                we have no tables in Egeria but we do have tables in Mysql
                so all tables are new
                 */
                if (egeriaTables == null)
                {
                    if( MysqlTables.size() > 0 )
                    {
                        addTable(MysqlTable, schemaGuid);
                    }
                }
                else
                {
                    /*
                    check if the database table is known to Egeria
                    and needs to be updated
                     */
                    for (DatabaseTableElement egeriaTable : egeriaTables)
                    {
                        if (egeriaTable.getDatabaseTableProperties().getQualifiedName().equals(MysqlTable.getQualifiedName()))
                        {
                        /*
                        we have found an exact instance to update
                         */
                            found = true;
                            updateTable(MysqlTable, egeriaTable);
                            break;
                        }
                    }
                    /*
                    this is a new database so add it
                     */
                    if (!found)
                    {
                       addTable(MysqlTable, schemaGuid);
                    }
                }
            }
        }
        catch (SQLException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.ERROR_READING_MYSQL.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.ERROR_READING_FROM_MYSQL.getMessageDefinition(methodName));

        }
        catch (InvalidParameterException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (UserNotAuthorizedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (PropertyServerException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(error.getClass().getName()));
        }

        catch (ConnectorCheckedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (Exception error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.UNEXPECTED_ERROR.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.UNEXPECTED_ERROR.getMessageDefinition(methodName));

        }
    }

    /**
     * @param MysqlTable  the Mysql table attributes to be added
     * @param egeriaTable    the GUID of the schema to which the table will be linked
     * @throws AlreadyHandledException this exception has already been logged
     */
    private void updateTable(MysqlTable MysqlTable, DatabaseTableElement egeriaTable) throws AlreadyHandledException
    {
        String methodName = "updateTable";

        try
        {
            if( MysqlTable.isEquivalent( egeriaTable) )
            {
                DatabaseTableProperties props = MysqlMapper.getTableProperties(MysqlTable);
                getContext().updateDatabaseTable(egeriaTable.getElementHeader().getGUID(), props);
            }

            updateTableColumns(MysqlTable, egeriaTable);
        }
        catch (InvalidParameterException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName));

        }
        catch (PropertyServerException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName));

        }
        catch (UserNotAuthorizedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName));

        }
        catch (ConnectorCheckedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName),
                    MysqlConnectorErrorCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(error.getClass().getName()));
        }
        catch (Exception error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.UNEXPECTED_ERROR.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.UNEXPECTED_ERROR.getMessageDefinition(methodName));

        }

    }


    /**
     * @param MysqlSchema the Mysql schema bean
     * @param egeriaSchema   the Egeria schema bean
     * @throws AlreadyHandledException this exception has already been logged
     */

    private void updateViews(MysqlSchema MysqlSchema, DatabaseSchemaElement egeriaSchema) throws AlreadyHandledException
    {
        final String methodName = "updateViews";

        String schemaGuid = egeriaSchema.getElementHeader().getGUID();
        MysqlSourceDatabase source = new MysqlSourceDatabase(this.connectionProperties);

        try
        {
            /*
            get a list of databases views currently hosted in Mysql
            and remove any tables that have been dropped since the last refresh
             */
            List<MysqlTable> MysqlViews = source.getViews(MysqlSchema.getSchema_name());
            List<DatabaseViewElement> egeriaViews = getContext().getViewsForDatabaseSchema(schemaGuid, startFrom, pageSize);

            egeriaViews = deleteViews( MysqlViews, egeriaViews);
            for (MysqlTable MysqlView : MysqlViews)
            {
                boolean found = false;
                /*
                we have no views in Egeria
                so all views are new
                 */
                if (egeriaViews == null)
                {
                    if( MysqlViews.size() > 0)
                    {
                        addView(MysqlView, schemaGuid);
                    }
                }
                else
                {
                    /*
                    check if the database table is known to Egeria
                    and needs to be updated
                     */
                    for (DatabaseViewElement egeriaView : egeriaViews)
                    {
                        if (egeriaView.getDatabaseViewProperties().getQualifiedName().equals(MysqlView.getQualifiedName()))
                        {
                        /*
                        we have found an exact instance to update
                         */
                            found = true;
                            updateView(MysqlView, egeriaView);
                            break;
                        }
                    }
                    /*
                    this is a new database view so add it
                     */
                    if (!found)
                    {
                        addView(MysqlView, schemaGuid);
                    }
                }
            }
        }
        catch (SQLException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.ERROR_READING_MYSQL.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.ERROR_READING_FROM_MYSQL.getMessageDefinition(methodName));

        }
        catch (InvalidParameterException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (UserNotAuthorizedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (PropertyServerException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName));
        }

        catch (ConnectorCheckedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (Exception error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.UNEXPECTED_ERROR.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.UNEXPECTED_ERROR.getMessageDefinition(methodName));

        }
    }

    /**
     * @param MysqlTable         the Mysql table attributes to be added
     * @param egeriaView    te GUID of the schema to which the table will be linked
     * @throws AlreadyHandledException this exception has already been logged
     */
    private void updateView(MysqlTable MysqlTable, DatabaseViewElement egeriaView) throws AlreadyHandledException
    {
        String methodName = "updateView";

        try
        {
            if( !MysqlTable.isEquivalent(egeriaView) )
            {
                DatabaseViewProperties props = MysqlMapper.getViewProperties(MysqlTable);
                getContext().updateDatabaseView(egeriaView.getElementHeader().getGUID(), props);
            }
            updateViewColumns(MysqlTable, egeriaView);
        }
        catch (InvalidParameterException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName));

        }
        catch (PropertyServerException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName));

        }
        catch (UserNotAuthorizedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName));

        }
        catch (ConnectorCheckedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(error.getClass().getName()));
        }
        catch (Exception error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.UNEXPECTED_ERROR.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.UNEXPECTED_ERROR.getMessageDefinition(methodName));

        }

    }


    /**
     * @param MysqlTable         the Mysql table which contains the columns to be updates
     * @param  egeriaTable  the column data from Egeria
     * @throws AlreadyHandledException this exception has already been logged
     */
    private void updateTableColumns(MysqlTable MysqlTable, DatabaseTableElement egeriaTable) throws AlreadyHandledException
    {
        final String methodName = "updateTableColumns";
        MysqlSourceDatabase source = new MysqlSourceDatabase(this.connectionProperties);
        String tableGuid = egeriaTable.getElementHeader().getGUID();
        try
        {
            List<MysqlColumn> MysqlColumns = source.getColumns(MysqlTable.getTable_name());
            List<DatabaseColumnElement> egeriaColumns = getContext().getColumnsForDatabaseTable(tableGuid, startFrom, pageSize);
            List<String> primarykeys = source.getPrimaryKeyColumnNamesForTable( MysqlTable.getTable_name());

                if( egeriaColumns != null && MysqlColumns.size() > 0)
                {
                    egeriaColumns = deleteTableColumns(MysqlColumns, egeriaColumns);
                }

                for (MysqlColumn MysqlColumn : MysqlColumns)
                {
                    boolean found = false;
                    /*
                    we have no columns in Egeria
                    so all columns are new
                     */
                    if (egeriaColumns == null)
                    {
                        if( MysqlColumns.size() > 0 )
                        {
                            addColumn(MysqlColumn, tableGuid);
                        }
                    }
                    else
                    {
                        /*
                        check if the database table is known to Egeria
                        and needs to be updated
                         */
                        for (DatabaseColumnElement egeriaColumn : egeriaColumns)
                        {
                            if (egeriaColumn.getDatabaseColumnProperties().getQualifiedName().equals(MysqlColumn.getQualifiedName()))
                            {
                            /*
                            we have found an exact instance to update
                             */
                                found = true;
                                //TODO
                             //   updateColumn(MysqlColumn, egeriaColumn);
                                break;
                            }

                            if( primarykeys.contains(egeriaColumn.getDatabaseColumnProperties().getDisplayName() ))
                            {
                                DatabasePrimaryKeyProperties props = new DatabasePrimaryKeyProperties();
                                getContext().setPrimaryKeyOnColumn(egeriaColumn.getElementHeader().getGUID(), props);
                            }
                            else
                            {
                                //was this a primary key previously.
                                if( egeriaColumn.getPrimaryKeyProperties() != null )
                                {
                                    getContext().removePrimaryKeyFromColumn( egeriaColumn.getElementHeader().getGUID());
                                }

                            }

                        }
                        /*
                        this is a new database so add it
                         */
                        if (!found)
                        {
                           addColumn(MysqlColumn, tableGuid);
                        }
                    }
                }
        }
        catch (SQLException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.ERROR_READING_MYSQL.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.ERROR_READING_FROM_MYSQL.getMessageDefinition(methodName));

        }
        catch (InvalidParameterException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName));

        }
        catch (UserNotAuthorizedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (PropertyServerException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (ConnectorCheckedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (Exception error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.UNEXPECTED_ERROR.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.UNEXPECTED_ERROR.getMessageDefinition(methodName));

        }

    }

    /**
     * @param MysqlTable         the Mysql table which contains the columns to be updates
     * @param  egeriaTable  the column data from Egeria
     * @throws AlreadyHandledException this exception has already been logged
     */
    private void updateViewColumns(MysqlTable MysqlTable, DatabaseViewElement egeriaTable) throws AlreadyHandledException
    {
        final String methodName = "updateViewColumns";

        MysqlSourceDatabase source = new MysqlSourceDatabase(this.connectionProperties);
        String guid = egeriaTable.getElementHeader().getGUID();
        try
        {
            List<MysqlColumn> MysqlColumns = source.getColumns(MysqlTable.getTable_name());
            List<DatabaseColumnElement> egeriaColumns = getContext().getColumnsForDatabaseTable(egeriaTable.getElementHeader().getGUID(), startFrom, pageSize);

            if( egeriaColumns != null )
            {
                egeriaColumns= deleteViewColumns(MysqlColumns, egeriaColumns);
            }

            for (MysqlColumn MysqlColumn : MysqlColumns)
            {
                boolean found = false;
                /*
                we have no tables in Egeria
                so all tables are new
                 */
                if (egeriaColumns == null)
                {
                    if(MysqlColumns.size() > 0 )
                    {
                        addColumn(MysqlColumn, guid);
                    }
                }
                else
                {
                    /*
                    check if the database table is known to Egeria
                    and needs to be updated
                     */
                    for (DatabaseColumnElement egeriaColumn : egeriaColumns)
                    {
                        if (egeriaColumn.getDatabaseColumnProperties().getQualifiedName().equals(MysqlColumn.getQualifiedName()))
                        {
                        /*
                        we have found an exact instance to update
                         */
                            found = true;
                            updateColumn(MysqlColumn, egeriaColumn);
                            break;
                        }

                    }
                    /*
                    this is a new column so add it
                     */
                    if (!found)
                    {
                        addColumn(MysqlColumn, guid);
                    }

                }


            }

        }
        catch (SQLException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.ERROR_READING_MYSQL.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.ERROR_READING_FROM_MYSQL.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()));

        }
        catch (InvalidParameterException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName));

        }
        catch (UserNotAuthorizedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (PropertyServerException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (ConnectorCheckedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (Exception error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.UNEXPECTED_ERROR.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.UNEXPECTED_ERROR.getMessageDefinition(methodName));

        }

    }


    /**
     * @param MysqlCol           the Mysql column
     * @param  egeriaCol            the column data from Egeria
     * @throws AlreadyHandledException this exception has already been logged
     */
    private void updateColumn(MysqlColumn MysqlCol, DatabaseColumnElement egeriaCol ) throws AlreadyHandledException
    {
        String methodName = "updateColumn";

        try
        {
            if( !MysqlCol.isEquivalent( egeriaCol))
            {
                DatabaseColumnProperties props = MysqlMapper.getColumnProperties( MysqlCol );
                getContext().updateDatabaseColumn(egeriaCol.getElementHeader().getGUID(), props);
            }

        }
        catch (InvalidParameterException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (PropertyServerException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName));

        }
        catch (UserNotAuthorizedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName));

        }
        catch (ConnectorCheckedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (Exception error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.UNEXPECTED_ERROR.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.UNEXPECTED_ERROR.getMessageDefinition(methodName));

        }

    }

    /**
     * mapping function that reads tables, columns and primary keys
     * for a schema from Mysql and adds the data to Egeria
     *
     * @param db the Mysql attributes of the database
     * @throws AlreadyHandledException this exception has already been logged
     */
    private void addDatabase(MysqlDatabase db) throws AlreadyHandledException
    {
        String methodName = "addDatabase";
      try
        {
         /*
         new database so build the database in Egeria
         */
            DatabaseProperties dbProps = MysqlMapper.getDatabaseProperties(db);
            String guid = this.getContext().createDatabase(dbProps);
            addSchemas(db.getName(), guid);

        }
        catch (InvalidParameterException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName));

        }
        catch (PropertyServerException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName));

        }
        catch (UserNotAuthorizedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (ConnectorCheckedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName));
        }
      catch (Exception error)
      {
          ExceptionHandler.handleException(auditLog,
                  this.getClass().getName(),
                  methodName, error,
                  MysqlConnectorAuditCode.UNEXPECTED_ERROR.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                  MysqlConnectorErrorCode.UNEXPECTED_ERROR.getMessageDefinition(methodName));

      }
    }

    /**
     * Adds schema entities to Egeria for a given database
     *
     * @param dbName the name of the database
     * @param dbGUID the GUID of the database entity to attach the schemas
     * @throws AlreadyHandledException this exception has already been logged
     */
    private void addSchemas(String dbName, String dbGUID) throws AlreadyHandledException
    {

        String methodName = "addSchemas";

        try
        {
            MysqlSourceDatabase sourceDB = new MysqlSourceDatabase(this.connectionProperties);
            List<MysqlSchema> schemas = sourceDB.getDatabaseSchema(dbName);
            for (MysqlSchema sch : schemas)
            {
                addSchema(sch, dbGUID);
            }

        } catch (SQLException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.ERROR_READING_MYSQL.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.ERROR_READING_FROM_MYSQL.getMessageDefinition(methodName));
        }
    }

    /**
     * mapping function that reads tables, columns and primary keys
     * for a schema from Mysql and adds the data to Egeria
     *
     * @param sch     the Mysql schema attributes to be
     * @param dbGuidd the Egeria GUID of the database
     * @throws AlreadyHandledException this exception has already been logged
     */
    private void addSchema(MysqlSchema sch, String dbGuidd) throws AlreadyHandledException
    {
        String methodName = "addSchema";

        try
        {
            DatabaseSchemaProperties schemaProps = MysqlMapper.getSchemaProperties(sch);

            String schemaGUID = getContext().createDatabaseSchema(dbGuidd, schemaProps);
            addTables(sch.getSchema_name(), schemaGUID);
            addViews( sch.getSchema_name(), schemaGUID);
            addForeignKeys(sch);
        }
        catch (InvalidParameterException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName));

        }
        catch (PropertyServerException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName));

        }
        catch (UserNotAuthorizedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (ConnectorCheckedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (Exception error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.UNEXPECTED_ERROR.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.UNEXPECTED_ERROR.getMessageDefinition(methodName));

        }
    }

    /**
     * mapping function that reads tables, columns and primary keys
     * for a schema from Mysql and adds the data to Egeria
     *
     * @param schemaName the attributes of the schema which owns the tables
     * @param schemaGUID the GUID of the owning schema
     * @throws AlreadyHandledException this exception has already been logged
     */
    private void addTables(String schemaName, String schemaGUID) throws AlreadyHandledException
    {
        String methodName = "addTables";

        MysqlSourceDatabase source = new MysqlSourceDatabase(this.connectionProperties);

        try
        {
            /* add the schema tables */
            List<MysqlTable> tables = source.getTables(schemaName);
            for (MysqlTable table : tables)
            {
                addTable(table, schemaGUID);
            }
        }
        catch (SQLException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.ERROR_READING_MYSQL.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.ERROR_READING_FROM_MYSQL.getMessageDefinition(methodName));

        }
        catch (Exception error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.UNEXPECTED_ERROR.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.UNEXPECTED_ERROR.getMessageDefinition(methodName));

        }

    }

    /**
     * creates an Egeria DatabaseTable entity for a given Mysql Table
     *
     * @param table      the Mysql schema attributes to be
     * @param schemaGUID the Egeria GUID of the schema
     * @throws AlreadyHandledException this exception has already been logged
     */
    private void addTable(MysqlTable table, String schemaGUID) throws AlreadyHandledException
    {
        String methodName = "addTable";

        try
        {
            DatabaseTableProperties props = MysqlMapper.getTableProperties(table);
            String tableGUID = this.getContext().createDatabaseTable(schemaGUID, props);
            addColumns(table.getTable_name(), tableGUID);
        }
        catch (InvalidParameterException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName));

        }
        catch (PropertyServerException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName));

        }
        catch (UserNotAuthorizedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (ConnectorCheckedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (Exception error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.UNEXPECTED_ERROR.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.UNEXPECTED_ERROR.getMessageDefinition(methodName));

        }

    }

    /**
     * creates an Egeria DatabaseView entity for a given Mysql Table
     * in Mysql views are tables
     *
     * @param view       the Mysql view properties
     * @param schemaGUID the Egeria GUID of the schema
     * @throws AlreadyHandledException this exception has already been logged
     */
    private void addView(MysqlTable view, String schemaGUID) throws AlreadyHandledException
    {
        String methodName = "addView";

        try
        {
            DatabaseViewProperties props = MysqlMapper.getViewProperties(view);
            String tableGUID = this.getContext().createDatabaseView(schemaGUID, props);
            addColumns(view.getTable_name(), tableGUID);
        } catch (InvalidParameterException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName));

        } catch (PropertyServerException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName));

        } catch (UserNotAuthorizedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName));
        } catch (ConnectorCheckedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (Exception error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.UNEXPECTED_ERROR.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.UNEXPECTED_ERROR.getMessageDefinition(methodName));

        }

    }


    /**
     * add the foreign keys to Egeria
     * for a schema from Mysql and adds the data to Egeria
     *
     * @param schema the attributes of the schema which owns the tables
     * @throws AlreadyHandledException this exception has already been logged
     */
    private void addForeignKeys(MysqlSchema schema) throws AlreadyHandledException
    {
        String methodName = "addForeignKeys";

        MysqlSourceDatabase source = new MysqlSourceDatabase(this.connectionProperties);

        try
        {

            List<MysqlTable> tables = source.getTables(schema.getSchema_name());
            for (MysqlTable table : tables)
            {
                List<MysqlForeignKeyLinks> foreignKeys = source.getForeginKeyLinksForTable(table.getTable_name());
                List<String> importedGuids = new ArrayList<>();
                List<String> exportedGuids = new ArrayList<>();

                for (MysqlForeignKeyLinks link : foreignKeys)
                {
                    List<DatabaseColumnElement> importedEntities = getContext().findDatabaseColumns(link.getImportedColumnQualifiedName(), startFrom, pageSize);

                    if (importedEntities != null)
                    {
                        for (DatabaseColumnElement col : importedEntities)
                        {
                            importedGuids.add(col.getReferencedColumnGUID());
                        }
                    }

                    List<DatabaseColumnElement> exportedEntities = this.getContext().findDatabaseColumns(link.getExportedColumnQualifiedName(), startFrom, pageSize);

                    if (exportedEntities != null)
                    {
                        for (DatabaseColumnElement col : exportedEntities)
                        {
                            exportedGuids.add(col.getReferencedColumnGUID());
                        }
                    }


                    for (String str : importedGuids)
                    {
                        DatabaseForeignKeyProperties linkProps = new DatabaseForeignKeyProperties();
                        for (String s : exportedGuids)
                            getContext().addForeignKeyRelationship(str, s, linkProps);
                    }

                }
            }
        }
        catch (SQLException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.ERROR_READING_MYSQL.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.ERROR_READING_FROM_MYSQL.getMessageDefinition(methodName));

        }
        catch (InvalidParameterException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName));

        }
        catch (PropertyServerException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName));

        }
        catch (UserNotAuthorizedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (ConnectorCheckedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (Exception error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.UNEXPECTED_ERROR.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.UNEXPECTED_ERROR.getMessageDefinition(methodName));

        }
    }

    /**
     * mapping function that reads tables, columns and primary keys
     * for a schema from Mysql and adds the data to Egeria
     *
     * @param schemaName the attributes of the schema which owns the tables
     * @param schemaGUID the GUID of the owning schema
     * @throws AlreadyHandledException this exception has already been logged
     */
    private void addViews(String schemaName, String schemaGUID) throws AlreadyHandledException
    {
        String methodName = "addViews";

        MysqlSourceDatabase source = new MysqlSourceDatabase(this.connectionProperties);

        try
        {
            List<MysqlTable> views = source.getViews(schemaName);

            for (MysqlTable view : views)
            {
                addView(view, schemaGUID);
            }


        } catch (SQLException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.ERROR_READING_MYSQL.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.ERROR_READING_FROM_MYSQL.getMessageDefinition(methodName));

        }
        catch (Exception error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.UNEXPECTED_ERROR.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.UNEXPECTED_ERROR.getMessageDefinition(methodName));

        }
    }

    /**
     * mapping function that reads tables, columns and primary keys
     * for a schema from Mysql and adds the data to Egeria
     *
     * @param tableName the name of the parent table
     * @param tableGUID the GUID of the owning table
     * @throws AlreadyHandledException this exception has already been logged
     */
    private void addColumns(String tableName, String tableGUID) throws AlreadyHandledException
    {
        String methodName = "addColumns";

        MysqlSourceDatabase source = new MysqlSourceDatabase(this.connectionProperties);
        try
        {
            List<MysqlColumn> cols = source.getColumns(tableName);

            for (MysqlColumn col : cols)
            {
                addColumn(col, tableGUID);
            }
        } catch (SQLException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.ERROR_READING_MYSQL.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.ERROR_READING_FROM_MYSQL.getMessageDefinition(methodName));

        }
        catch (Exception error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.UNEXPECTED_ERROR.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.UNEXPECTED_ERROR.getMessageDefinition(methodName));

        }

    }

    /**
     * mapping function that reads columns and primary keys
     * for a schema from Mysql and creates
     *
     * @param col         the Mysql attributes of the column
     * @param guid        the GUID of the owning table
     * @throws AlreadyHandledException allows the exception to be passed up the stack, without additional handling
     */
    private void addColumn(MysqlColumn col, String guid) throws AlreadyHandledException
    {
        String methodName = "addColumn";

        try
        {
            DatabaseColumnProperties colProps = MysqlMapper.getColumnProperties(col);
            this.getContext().createDatabaseColumn(guid, colProps);

        }
        catch (InvalidParameterException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (PropertyServerException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName));

        }
        catch (UserNotAuthorizedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (ConnectorCheckedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (Exception error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.UNEXPECTED_ERROR.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.UNEXPECTED_ERROR.getMessageDefinition(methodName));

        }
    }





    /**
     * Checks if any databases need to be removed from Egeria
     *
     * @param MysqlDatabases            a list of the bean properties of a Mysql Database
     * @param egeriaDatabases    a list of the Databases already known to Egeria
     * @throws AlreadyHandledException this exception has already been logged
     */
    private  List<DatabaseElement> deleteDatabases(List<MysqlDatabase> MysqlDatabases, List<DatabaseElement> egeriaDatabases) throws AlreadyHandledException
    {
        String methodName = "deleteDatabases";

        try
        {
            if (egeriaDatabases != null)
            {
                /*
                for each database already known to Egeria
                 */
                for (Iterator<DatabaseElement> itr = egeriaDatabases.iterator(); itr.hasNext();)
                {
                    boolean found = false;
                    DatabaseElement egeriaDatabase = itr.next();
                    String knownName = egeriaDatabase.getDatabaseProperties().getQualifiedName();
                    /*
                    check that the database is still present in Mysql
                     */
                    for (MysqlDatabase MysqlDatabase : MysqlDatabases)
                    {
                        String sourceName = MysqlDatabase.getQualifiedName();
                        if (sourceName.equals(knownName))
                        {
                            /*
                            if found then check the next database
                             */
                            found = true;
                            break;
                        }
                    }
                        /*
                        not found in Mysql , so delete the database from Egeria
                         */
                    if( !found)
                    {
                        getContext().removeDatabase(egeriaDatabase.getElementHeader().getGUID(), knownName);
                        itr.remove();
                    }

                }
            }

        }
        catch (InvalidParameterException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (PropertyServerException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (UserNotAuthorizedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName));

        }
        catch (ConnectorCheckedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (Exception error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.UNEXPECTED_ERROR.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.UNEXPECTED_ERROR.getMessageDefinition(methodName));

        }

        return egeriaDatabases;
    }

    /**
     * Checks if any schemas need to be removed from Egeria
     *
     * @param MysqlSchemas            a list of the bean properties of a Mysql schemas
     * @param egeriaSchemas    a list of the Databases already known to Egeria
     * @throws AlreadyHandledException this exception has already been logged
     */
    private List<DatabaseSchemaElement> deleteSchemas(List<MysqlSchema> MysqlSchemas, List<DatabaseSchemaElement> egeriaSchemas) throws AlreadyHandledException
    {
        String methodName = "deleteSchemas";

        try
        {
            if (egeriaSchemas != null)
            {
                /*
                for each schema already known to Egeria
                 */
                for (Iterator<DatabaseSchemaElement> itr = egeriaSchemas.iterator(); itr.hasNext();)
                {
                    boolean found = false;
                    DatabaseSchemaElement egeriaSchema = itr.next();

                    String knownName = egeriaSchema.getDatabaseSchemaProperties().getQualifiedName();
                    /*
                    check that the database is still present in Mysql
                     */
                    for (MysqlSchema MysqlSchema : MysqlSchemas)
                    {
                        String sourceName = MysqlSchema.getQualifiedName();
                        if (sourceName.equals(knownName))
                        {
                            /*
                            if found then check the next schema
                             */
                            found = true;
                            break;
                        }
                    }
                        /*
                        not found in Mysql , so delete the schema from Egeria
                         */
                    if( !found)
                    {
                        getContext().removeDatabaseSchema(egeriaSchema.getElementHeader().getGUID(), knownName);
                        itr.remove();
                    }

                }
            }
        }
        catch (InvalidParameterException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (PropertyServerException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (UserNotAuthorizedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName));

        }
        catch (ConnectorCheckedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (Exception error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.UNEXPECTED_ERROR.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.UNEXPECTED_ERROR.getMessageDefinition(methodName));

        }

        return egeriaSchemas;
    }

    /**
     * Checks if any schemas need to be removed from Egeria
     *
     * @param MysqlTables            a list of the bean properties of a Mysql schemas
     * @param egeriaTables    a list of the Databases already known to Egeria
     * @throws AlreadyHandledException this exception has already been logged
     */
    private List<DatabaseTableElement> deleteTables(List<MysqlTable> MysqlTables, List<DatabaseTableElement> egeriaTables) throws AlreadyHandledException
    {
        String methodName = "deleteTables";
        try
        {
            if (egeriaTables != null)
            {
                /*
                for each table already known to Egeria
                 */
                for (Iterator<DatabaseTableElement> itr = egeriaTables.iterator(); itr.hasNext();)
                {
                    boolean found = false;
                    DatabaseTableElement egeriaTable = itr.next();
                    String knownName = egeriaTable.getDatabaseTableProperties().getQualifiedName();
                    /*
                    check that the database is still present in Mysql
                     */
                    for (MysqlTable MysqlTable : MysqlTables)
                    {
                        String sourceName = MysqlTable.getQualifiedName();
                        if (sourceName.equals(knownName))
                        {
                            /*
                            if found then check the next table
                             */
                            found = true;
                            break;
                        }
                    }
                        /*
                        not found in Mysql , so delete the table from Egeria
                         */
                    if( !found)
                    {
                        getContext().removeDatabaseTable(egeriaTable.getElementHeader().getGUID(), knownName);
                        itr.remove();
                    }

                }
            }
        }
        catch (InvalidParameterException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (PropertyServerException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (UserNotAuthorizedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName));

        }
        catch (ConnectorCheckedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (Exception error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.UNEXPECTED_ERROR.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.UNEXPECTED_ERROR.getMessageDefinition(methodName));

        }

        return egeriaTables;
    }

    /**
     * Checks if any views need to be removed from Egeria
     *
     * @param MysqlViews            a list of the bean properties of a Mysql views
     * @param egeriaViews               a list of the  views already known to Egeria
     * @throws AlreadyHandledException this exception has already been logged
     */
    private List<DatabaseViewElement> deleteViews(List<MysqlTable> MysqlViews, List<DatabaseViewElement> egeriaViews) throws AlreadyHandledException
    {
        String methodName = "deleteViews";

        try
        {
            if (egeriaViews != null)
            {
                /*
                for each view already known to Egeria
                 */
                for (Iterator<DatabaseViewElement> itr = egeriaViews.iterator(); itr.hasNext();)
                {
                    boolean found = false;
                    DatabaseViewElement egeriaView = itr.next();

                    String knownName = egeriaView.getDatabaseViewProperties().getQualifiedName();
                    /*
                    check that the database is still present in Mysql
                     */
                    for (MysqlTable MysqlView : MysqlViews)
                    {
                        String sourceName = MysqlView.getQualifiedName();
                        if (sourceName.equals(knownName))
                        {
                            /*
                            if found then check the next table
                             */
                            found = true;
                            break;
                        }
                    }
                        /*
                        not found in Mysql , so delete the table from Egeria
                         */
                    if( !found)
                    {
                        getContext().removeDatabaseView(egeriaView.getElementHeader().getGUID(), knownName);
                        itr.remove();
                    }

                }
            }
        }
        catch (InvalidParameterException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (PropertyServerException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (UserNotAuthorizedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName));

        }
        catch (ConnectorCheckedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (Exception error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.UNEXPECTED_ERROR.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.UNEXPECTED_ERROR.getMessageDefinition(methodName));

        }

        return egeriaViews;
    }


    /**
     * Checks if any columns need to be removed from Egeria
     *
     * @param MysqlColumns            a list of the bean properties of a Mysql cols
     * @param egeriaColumns               a list of the  cols already known to Egeria
     * @throws AlreadyHandledException this exception has already been logged
     */
    private List<DatabaseColumnElement> deleteTableColumns(List<MysqlColumn> MysqlColumns, List<DatabaseColumnElement> egeriaColumns) throws AlreadyHandledException
    {
        String methodName = "deleteTableColumns";

        try
        {
            if (egeriaColumns != null)
            {
                /*
                for each column already known to Egeria
                 */
                for (Iterator<DatabaseColumnElement> itr = egeriaColumns.iterator(); itr.hasNext();)
                {
                    boolean found = false;
                    DatabaseColumnElement egeriaColumn = itr.next();

                    String knownName = egeriaColumn.getDatabaseColumnProperties().getQualifiedName();
                    /*
                    check that the database is still present in Mysql
                     */
                    for (MysqlColumn MysqlColumn : MysqlColumns)
                    {
                        String sourceName = MysqlColumn.getQualifiedName();
                        if (sourceName.equals(knownName))
                        {
                            /*
                            if found then check the next column
                             */
                            found = true;
                            break;
                        }
                    }
                        /*
                        not found in Mysql , so delete the table from Egeria
                         */
                    if( !found)
                    {
                        getContext().removeDatabaseView(egeriaColumn.getElementHeader().getGUID(), knownName);
                        itr.remove();
                    }

                }
            }
        }
        catch (InvalidParameterException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (PropertyServerException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (UserNotAuthorizedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName));

        }
        catch (ConnectorCheckedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (Exception error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.UNEXPECTED_ERROR.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.UNEXPECTED_ERROR.getMessageDefinition(methodName));

        }

        return egeriaColumns;
    }


    /**
     * Checks if any columns need to be removed from Egeria
     *
     * @param MysqlColumns            a list of the bean properties of a Mysql cols
     * @param egeriaColumns               a list of the  cols already known to Egeria
     * @throws AlreadyHandledException this exception has already been logged
     */
    private List<DatabaseColumnElement> deleteViewColumns(List<MysqlColumn> MysqlColumns, List<DatabaseColumnElement> egeriaColumns) throws AlreadyHandledException
    {
        String methodName = "deleteViewColumns";

        try
        {
            if (egeriaColumns != null)
            {
                /*
                for each column already known to Egeria
                 */
                for (Iterator<DatabaseColumnElement> itr = egeriaColumns.iterator(); itr.hasNext();)
                {
                    boolean found = false;
                    DatabaseColumnElement egeriaColumn = itr.next();

                    String knownName = egeriaColumn.getDatabaseColumnProperties().getQualifiedName();
                    /*
                    check that the database is still present in Mysql
                     */
                    for (MysqlColumn MysqlColumn : MysqlColumns)
                    {
                        String sourceName = MysqlColumn.getQualifiedName();
                        if (sourceName.equals(knownName))
                        {
                            /*
                            if found then check the next column
                             */
                            found = true;
                            break;
                        }
                    }
                        /*
                        not found in Mysql , so delete the table from Egeria
                         */
                    if( !found)
                    {
                        getContext().removeDatabaseView(egeriaColumn.getElementHeader().getGUID(), knownName);
                        itr.remove();
                    }

                }
            }
        }
        catch (InvalidParameterException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (PropertyServerException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (UserNotAuthorizedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName));

        }
        catch (ConnectorCheckedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (Exception error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    MysqlConnectorAuditCode.UNEXPECTED_ERROR.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    MysqlConnectorErrorCode.UNEXPECTED_ERROR.getMessageDefinition(methodName));

        }
        return egeriaColumns;
    }
}
