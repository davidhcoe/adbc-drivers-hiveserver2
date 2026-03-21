/*
 * Copyright (c) 2025 ADBC Drivers Contributors
 *
 * This file has been modified from its original version, which is
 * under the Apache License:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.HiveServer2.Thrift;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Extensions;
using Apache.Arrow.Adbc.Telemetry.Traces.Listeners;
using Apache.Arrow.Adbc.Telemetry.Traces.Listeners.FileListener;
using Apache.Arrow.Adbc.Tracing;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Apache.Hive.Service.Rpc.Thrift;
using Thrift;
using Thrift.Protocol;
using Thrift.Transport;

namespace AdbcDrivers.HiveServer2.Hive2
{
    internal abstract class HiveServer2Connection : TracingConnection, IGetObjectsDataProvider
    {
        internal const bool InfoVendorSql = true;
        internal const long BatchSizeDefault = 50000;
        internal const bool EnableBatchSizeStopConditionDefault = false;
        internal const int PollTimeMillisecondsDefault = 500;
        internal static readonly string s_assemblyName = ApacheUtility.GetAssemblyName(typeof(HiveServer2Connection));
        internal static readonly string s_assemblyVersion = ApacheUtility.GetAssemblyVersion(typeof(HiveServer2Connection));
        private const int ConnectTimeoutMillisecondsDefault = 30000;
        private const string ClassName = nameof(HiveServer2Connection);
        private TTransport? _transport;
        private TCLIService.IAsync? _client;
        private readonly Lazy<string> _vendorVersion;
        private readonly Lazy<string> _vendorName;
        private bool _isDisposed;
        // Note: this needs to be set before the constructor runs
        private readonly string _traceInstanceId = Guid.NewGuid().ToString("N");
        private readonly FileActivityListener? _fileActivityListener;

        // Standard column name constants used by subclasses when mapping
        // metadata result sets (e.g. in GetColumnsMetadataColumnNames).
        internal const string ColumnDef = MetadataColumnNames.ColumnDef;
        internal const string ColumnName = MetadataColumnNames.ColumnName;
        internal const string DataType = MetadataColumnNames.DataType;
        internal const string IsAutoIncrement = MetadataColumnNames.IsAutoIncrement;
        internal const string IsNullable = MetadataColumnNames.IsNullable;
        internal const string OrdinalPosition = MetadataColumnNames.OrdinalPosition;
        internal const string TableCat = MetadataColumnNames.TableCat;
        internal const string TableCatalog = MetadataColumnNames.TableCatalog;
        internal const string TableName = MetadataColumnNames.TableName;
        internal const string TableSchem = MetadataColumnNames.TableSchem;
        internal const string TableMd = MetadataColumnNames.TableMd;
        internal const string TableType = MetadataColumnNames.TableType;
        internal const string TypeName = MetadataColumnNames.TypeName;
        internal const string Nullable = MetadataColumnNames.Nullable;
        internal const string ColumnSize = MetadataColumnNames.ColumnSize;
        internal const string DecimalDigits = MetadataColumnNames.DecimalDigits;
        internal const string BufferLength = MetadataColumnNames.BufferLength;

        readonly AdbcInfoCode[] infoSupportedCodes = [
            AdbcInfoCode.DriverName,
            AdbcInfoCode.DriverVersion,
            AdbcInfoCode.DriverArrowVersion,
            AdbcInfoCode.VendorName,
            AdbcInfoCode.VendorSql,
            AdbcInfoCode.VendorVersion,
        ];


        /// <summary>
        /// The GetColumns metadata call returns a result with different column names
        /// on different data sources. Populate this structure with the actual column names.
        /// </summary>
        internal struct ColumnsMetadataColumnNames
        {
            public string TableCatalog { get; internal set; }
            public string TableSchema { get; internal set; }
            public string TableName { get; internal set; }
            public string ColumnName { get; internal set; }
            public string DataType { get; internal set; }
            public string TypeName { get; internal set; }
            public string Nullable { get; internal set; }
            public string ColumnDef { get; internal set; }
            public string OrdinalPosition { get; internal set; }
            public string IsNullable { get; internal set; }
            public string IsAutoIncrement { get; internal set; }
            public string ColumnSize { get; set; }
            public string DecimalDigits { get; set; }
        }

        /// <summary>
        /// The data type definitions based on the <see href="https://docs.oracle.com/en%2Fjava%2Fjavase%2F21%2Fdocs%2Fapi%2F%2F/java.sql/java/sql/Types.html">JDBC Types</see> constants.
        /// </summary>
        /// <remarks>
        /// This enumeration can be used to determine the drivers specific data types that are contained in fields <c>xdbc_data_type</c> and <c>xdbc_sql_data_type</c>
        /// in the column metadata <see cref="StandardSchemas.ColumnSchema"/>. This column metadata is returned as a result of a call to
        /// <see cref="AdbcConnection.GetObjects(GetObjectsDepth, string?, string?, string?, IReadOnlyList{string}?, string?)"/>
        /// when <c>depth</c> is set to <see cref="AdbcConnection.GetObjectsDepth.All"/>.
        /// </remarks>
        internal enum ColumnTypeId
        {
            // Please keep up-to-date.
            // Copied from https://docs.oracle.com/en%2Fjava%2Fjavase%2F21%2Fdocs%2Fapi%2F%2F/constant-values.html#java.sql.Types.ARRAY

            /// <summary>
            /// Identifies the generic SQL type ARRAY
            /// </summary>
            ARRAY = 2003,
            /// <summary>
            /// Identifies the generic SQL type BIGINT
            /// </summary>
            BIGINT = -5,
            /// <summary>
            /// Identifies the generic SQL type BINARY
            /// </summary>
            BINARY = -2,
            /// <summary>
            /// Identifies the generic SQL type BOOLEAN
            /// </summary>
            BOOLEAN = 16,
            /// <summary>
            /// Identifies the generic SQL type CHAR
            /// </summary>
            CHAR = 1,
            /// <summary>
            /// Identifies the generic SQL type DATE
            /// </summary>
            DATE = 91,
            /// <summary>
            /// Identifies the generic SQL type DECIMAL
            /// </summary>
            DECIMAL = 3,
            /// <summary>
            /// Identifies the generic SQL type DOUBLE
            /// </summary>
            DOUBLE = 8,
            /// <summary>
            /// Identifies the generic SQL type FLOAT
            /// </summary>
            FLOAT = 6,
            /// <summary>
            /// Identifies the generic SQL type INTEGER
            /// </summary>
            INTEGER = 4,
            /// <summary>
            /// Identifies the generic SQL type JAVA_OBJECT (MAP)
            /// </summary>
            JAVA_OBJECT = 2000,
            /// <summary>
            /// identifies the generic SQL type LONGNVARCHAR
            /// </summary>
            LONGNVARCHAR = -16,
            /// <summary>
            /// identifies the generic SQL type LONGVARBINARY
            /// </summary>
            LONGVARBINARY = -4,
            /// <summary>
            /// identifies the generic SQL type LONGVARCHAR
            /// </summary>
            LONGVARCHAR = -1,
            /// <summary>
            /// identifies the generic SQL type NCHAR
            /// </summary>
            NCHAR = -15,
            /// <summary>
            /// identifies the generic SQL type NULL
            /// </summary>
            NULL = 0,
            /// <summary>
            /// identifies the generic SQL type NUMERIC
            /// </summary>
            NUMERIC = 2,
            /// <summary>
            /// identifies the generic SQL type NVARCHAR
            /// </summary>
            NVARCHAR = -9,
            /// <summary>
            /// identifies the generic SQL type REAL
            /// </summary>
            REAL = 7,
            /// <summary>
            /// Identifies the generic SQL type SMALLINT
            /// </summary>
            SMALLINT = 5,
            /// <summary>
            /// Identifies the generic SQL type STRUCT
            /// </summary>
            STRUCT = 2002,
            /// <summary>
            /// Identifies the generic SQL type TIMESTAMP
            /// </summary>
            TIMESTAMP = 93,
            /// <summary>
            /// Identifies the generic SQL type TINYINT
            /// </summary>
            TINYINT = -6,
            /// <summary>
            /// Identifies the generic SQL type VARBINARY
            /// </summary>
            VARBINARY = -3,
            /// <summary>
            /// Identifies the generic SQL type VARCHAR
            /// </summary>
            VARCHAR = 12,
            // ======================
            // Unused/unsupported
            // ======================
            /// <summary>
            /// Identifies the generic SQL type BIT
            /// </summary>
            BIT = -7,
            /// <summary>
            /// Identifies the generic SQL type BLOB
            /// </summary>
            BLOB = 2004,
            /// <summary>
            /// Identifies the generic SQL type CLOB
            /// </summary>
            CLOB = 2005,
            /// <summary>
            /// Identifies the generic SQL type DATALINK
            /// </summary>
            DATALINK = 70,
            /// <summary>
            /// Identifies the generic SQL type DISTINCT
            /// </summary>
            DISTINCT = 2001,
            /// <summary>
            /// identifies the generic SQL type NCLOB
            /// </summary>
            NCLOB = 2011,
            /// <summary>
            /// Indicates that the SQL type is database-specific and gets mapped to a Java object
            /// </summary>
            OTHER = 1111,
            /// <summary>
            /// Identifies the generic SQL type REF CURSOR
            /// </summary>
            REF_CURSOR = 2012,
            /// <summary>
            /// Identifies the generic SQL type REF
            /// </summary>
            REF = 2006,
            /// <summary>
            /// Identifies the generic SQL type ROWID
            /// </summary>
            ROWID = -8,
            /// <summary>
            /// Identifies the generic SQL type XML
            /// </summary>
            SQLXML = 2009,
            /// <summary>
            /// Identifies the generic SQL type TIME
            /// </summary>
            TIME = 92,
            /// <summary>
            /// Identifies the generic SQL type TIME WITH TIMEZONE
            /// </summary>
            TIME_WITH_TIMEZONE = 2013,
            /// <summary>
            /// Identifies the generic SQL type TIMESTAMP WITH TIMEZONE
            /// </summary>
            TIMESTAMP_WITH_TIMEZONE = 2014,
        }

        internal HiveServer2Connection(IReadOnlyDictionary<string, string> properties)
            : base(properties)
        {
            Properties = properties;

            TryInitTracerProvider(out _fileActivityListener);

            // Note: "LazyThreadSafetyMode.PublicationOnly" is thread-safe initialization where
            // the first successful thread sets the value. If an exception is thrown, initialization
            // will retry until it successfully returns a value without an exception.
            // https://learn.microsoft.com/en-us/dotnet/framework/performance/lazy-initialization#exceptions-in-lazy-objects
            _vendorVersion = new Lazy<string>(() => GetInfoTypeStringValue(TGetInfoType.CLI_DBMS_VER), LazyThreadSafetyMode.PublicationOnly);
            _vendorName = new Lazy<string>(() => GetInfoTypeStringValue(TGetInfoType.CLI_DBMS_NAME), LazyThreadSafetyMode.PublicationOnly);

            if (properties.TryGetValue(ApacheParameters.QueryTimeoutSeconds, out string? queryTimeoutSecondsSettingValue))
            {
                if (ApacheUtility.QueryTimeoutIsValid(ApacheParameters.QueryTimeoutSeconds, queryTimeoutSecondsSettingValue, out int queryTimeoutSeconds))
                {
                    QueryTimeoutSeconds = queryTimeoutSeconds;
                }
            }
        }

        private bool TryInitTracerProvider(out FileActivityListener? fileActivityListener)
        {
            Properties.TryGetValue(ListenersOptions.Exporter, out string? exporterOption);
            // This listener will only listen for activity from this specific connection instance.
            bool shouldListenTo(ActivitySource source) => source.Tags?.Any(t => ReferenceEquals(t.Key, _traceInstanceId)) == true;
            return FileActivityListener.TryActivateFileListener(AssemblyName, exporterOption, out fileActivityListener, shouldListenTo: shouldListenTo);
        }

        public override IEnumerable<KeyValuePair<string, object?>>? GetActivitySourceTags(IReadOnlyDictionary<string, string> properties)
        {
            IEnumerable<KeyValuePair<string, object?>>? tags = base.GetActivitySourceTags(properties);
            tags ??= [];
            tags = tags.Concat([new(_traceInstanceId, null)]);
            return tags;
        }

        /// <summary>
        /// Conditional used to determines if it is safe to trace
        /// </summary>
        /// <remarks>
        /// It is safe to write to some output types (ie, files) but not others (ie, a shared resource).
        /// </remarks>
        /// <returns></returns>
        internal bool IsSafeToTrace => _fileActivityListener != null;

        internal TCLIService.IAsync Client
        {
            get { return _client ?? throw new InvalidOperationException("connection not open"); }
        }

        internal string VendorVersion => _vendorVersion.Value;

        internal string VendorName => _vendorName.Value;

        protected internal int QueryTimeoutSeconds { get; set; } = ApacheUtility.QueryTimeoutSecondsDefault;

        internal IReadOnlyDictionary<string, string> Properties { get; }

        internal async Task OpenAsync()
        {
            await this.TraceActivityAsync(async activity =>
            {
                CancellationToken cancellationToken = ApacheUtility.GetCancellationToken(ConnectTimeoutMilliseconds, ApacheUtility.TimeUnit.Milliseconds);
                try
                {
                    TTransport transport = CreateTransport();
                    TProtocol protocol = await CreateProtocolAsync(transport, cancellationToken);
                    _transport = protocol.Transport;
                    _client = CreateTCLIServiceClient(protocol);
                    TOpenSessionReq request = CreateSessionRequest();
                    TOpenSessionResp? session = null;
                    try
                    {
                        session = await Client.OpenSession(request, cancellationToken);
                    }
                    catch (TTransportException transportEx)
                        when (ApacheUtility.ContainsException(transportEx, out HttpRequestException? httpEx) && IsUnauthorized(httpEx!))
                    {
                        throw new HiveServer2Exception(transportEx.Message, AdbcStatusCode.Unauthorized, transportEx);
                    }
                    catch (Exception)
                    {
                        if (FallbackProtocolVersions.Any())
                        {
                            session = await TryOpenSessionWithFallbackAsync(request, cancellationToken);
                        }
                        else
                        {
                            throw;
                        }
                    }
                    await HandleOpenSessionResponse(session, activity);
                }
                catch (Exception ex) when (ExceptionHelper.IsOperationCanceledOrCancellationRequested(ex, cancellationToken))
                {
                    throw new TimeoutException("The operation timed out while attempting to open a session. Please try increasing connect timeout.", ex);
                }
                catch (Exception ex) when (ex is not HiveServer2Exception)
                {
                    // Handle other exceptions if necessary
                    throw new HiveServer2Exception($"An unexpected error occurred while opening the session. '{ApacheUtility.FormatExceptionMessage(ex)}'", ex);
                }
            }, ClassName + "." + nameof(OpenAsync));
        }

        private static bool IsUnauthorized(HttpRequestException httpEx)
        {
#if NET5_0_OR_GREATER
            return httpEx.StatusCode == HttpStatusCode.Unauthorized;
#else
            return httpEx.Message.IndexOf("unauthorized", StringComparison.OrdinalIgnoreCase) >= 0 || httpEx.Message.IndexOf("authenticat", StringComparison.OrdinalIgnoreCase) >= 0;
#endif
        }

        private async Task<TOpenSessionResp?> TryOpenSessionWithFallbackAsync(TOpenSessionReq originalRequest, CancellationToken cancellationToken)
        {
            Exception? lastException = null;

            foreach (var fallbackVersion in FallbackProtocolVersions)
            {
                try
                {
                    ResetConnection();
                    // Recreate transport + client
                    var retryTransport = CreateTransport();
                    var retryProtocol = await CreateProtocolAsync(retryTransport, cancellationToken);
                    _transport = retryProtocol.Transport;
                    _client = CreateTCLIServiceClient(retryProtocol);
                    // New request with fallback version
                    var retryReq = CreateSessionRequest();
                    retryReq.Client_protocol = fallbackVersion;

                    return await Client.OpenSession(retryReq, cancellationToken);
                }
                catch (Exception ex) when (ExceptionHelper.IsOperationCanceledOrCancellationRequested(ex, cancellationToken))
                {
                    throw new TimeoutException("The operation timed out while attempting to open a session. Please try increasing connect timeout.", ex);
                }
                catch (TTransportException transportEx)
                    when (ApacheUtility.ContainsException(transportEx, out HttpRequestException? httpEx) && IsUnauthorized(httpEx!))
                {
                    throw new HiveServer2Exception(transportEx.Message, AdbcStatusCode.Unauthorized, transportEx);
                }
                catch (Exception ex)
                {
                    lastException = ex;
                }
            }

            throw lastException ?? new HiveServer2Exception("Error occurred while opening the session. All protocol fallback attempts failed.");
        }

        private void ResetConnection()
        {
            try
            {
                _transport?.Close();
            }
            catch
            {
                // Ignore cleanup failure
            }

            _transport = null;
            _client = null;
        }

        protected virtual Task HandleOpenSessionResponse(TOpenSessionResp? session, Activity? activity = default)
        {
            // Explicitly check the session status
            if (session == null)
            {
                throw new HiveServer2Exception("Unable to open session. Unknown error.");
            }
            HandleThriftResponse(session.Status, activity);

            SessionHandle = session.SessionHandle;
            ServerProtocolVersion = session.ServerProtocolVersion;
            return Task.CompletedTask;
        }

        protected virtual TCLIService.IAsync CreateTCLIServiceClient(TProtocol protocol)
        {
            return new TCLIService.Client(protocol);
        }

        protected virtual IEnumerable<TProtocolVersion> FallbackProtocolVersions => Enumerable.Empty<TProtocolVersion>();

        internal TSessionHandle? SessionHandle { get; private set; }

        internal TProtocolVersion? ServerProtocolVersion { get; private set; }

        protected internal DataTypeConversion DataTypeConversion { get; set; } = DataTypeConversion.None;

        protected internal TlsProperties TlsOptions { get; set; } = new TlsProperties();

        protected internal int ConnectTimeoutMilliseconds { get; set; } = ConnectTimeoutMillisecondsDefault;

        protected abstract TTransport CreateTransport();

        protected abstract Task<TProtocol> CreateProtocolAsync(TTransport transport, CancellationToken cancellationToken = default);

        protected abstract TOpenSessionReq CreateSessionRequest();

        internal abstract SchemaParser SchemaParser { get; }

        internal abstract IArrowArrayStream NewReader<T>(
            T statement,
            Schema schema,
            IResponse response,
            TGetResultSetMetadataResp? metadataResp = null) where T : IHiveServer2Statement;

        public override IArrowArrayStream GetObjects(GetObjectsDepth depth, string? catalogPattern, string? dbSchemaPattern, string? tableNamePattern, IReadOnlyList<string>? tableTypes, string? columnNamePattern)
        {
            return this.TraceActivity(_ =>
            {
                if (SessionHandle == null)
                {
                    throw new InvalidOperationException("Invalid session");
                }

                CancellationToken cancellationToken = ApacheUtility.GetCancellationToken(QueryTimeoutSeconds, ApacheUtility.TimeUnit.Seconds);
                try
                {
                    if (GetObjectsPatternsRequireLowerCase)
                    {
                        catalogPattern = catalogPattern?.ToLower();
                        dbSchemaPattern = dbSchemaPattern?.ToLower();
                        tableNamePattern = tableNamePattern?.ToLower();
                        columnNamePattern = columnNamePattern?.ToLower();
                    }

                    return GetObjectsResultBuilder.BuildGetObjectsResultAsync(
                        this, depth, catalogPattern, dbSchemaPattern,
                        tableNamePattern, tableTypes, columnNamePattern,
                        cancellationToken).GetAwaiter().GetResult();
                }
                catch (Exception ex) when (ExceptionHelper.IsOperationCanceledOrCancellationRequested(ex, cancellationToken))
                {
                    throw new TimeoutException("The metadata query execution timed out. Consider increasing the query timeout value.", ex);
                }
                catch (Exception ex) when (ex is not HiveServer2Exception)
                {
                    throw new HiveServer2Exception($"An unexpected error occurred while running metadata query. '{ApacheUtility.FormatExceptionMessage(ex)}'", ex);
                }
            }, ClassName + "." + nameof(GetObjects));
        }

        // IGetObjectsDataProvider implementation

        async Task<IReadOnlyList<string>> IGetObjectsDataProvider.GetCatalogsAsync(string? catalogPattern, CancellationToken cancellationToken)
        {
            TGetCatalogsResp getCatalogsResp = await GetCatalogsAsync(cancellationToken).ConfigureAwait(false);

            var catalogsMetadata = await GetResultSetMetadataAsync(getCatalogsResp, cancellationToken).ConfigureAwait(false);
            IReadOnlyDictionary<string, int> columnMap = GetColumnIndexMap(catalogsMetadata.Schema.Columns);

            string catalogRegexp = PatternToRegEx(catalogPattern);
            TRowSet rowSet = await GetRowSetAsync(getCatalogsResp, cancellationToken).ConfigureAwait(false);
            IReadOnlyList<string> list = rowSet.Columns[columnMap[TableCat]].StringVal.Values;

            var result = new List<string>();
            for (int i = 0; i < list.Count; i++)
            {
                string catalog = list[i];
                if (Regex.IsMatch(catalog, catalogRegexp, RegexOptions.IgnoreCase))
                {
                    result.Add(catalog);
                }
            }
            if (list.Count == 0 && string.IsNullOrEmpty(catalogPattern))
            {
                result.Add(string.Empty);
            }
            return result;
        }

        async Task<IReadOnlyList<(string catalog, string schema)>> IGetObjectsDataProvider.GetSchemasAsync(string? catalogPattern, string? schemaPattern, CancellationToken cancellationToken)
        {
            TGetSchemasResp getSchemasResp = await GetSchemasAsync(catalogPattern, schemaPattern, cancellationToken).ConfigureAwait(false);

            TGetResultSetMetadataResp schemaMetadata = await GetResultSetMetadataAsync(getSchemasResp, cancellationToken).ConfigureAwait(false);
            IReadOnlyDictionary<string, int> columnMap = GetColumnIndexMap(schemaMetadata.Schema.Columns);
            TRowSet rowSet = await GetRowSetAsync(getSchemasResp, cancellationToken).ConfigureAwait(false);

            IReadOnlyList<string> catalogList = rowSet.Columns[columnMap[TableCatalog]].StringVal.Values;
            IReadOnlyList<string> schemaList = rowSet.Columns[columnMap[TableSchem]].StringVal.Values;

            var result = new List<(string, string)>();
            for (int i = 0; i < catalogList.Count; i++)
            {
                result.Add((catalogList[i], schemaList[i]));
            }
            return result;
        }

        async Task<IReadOnlyList<(string catalog, string schema, string table, string tableType)>> IGetObjectsDataProvider.GetTablesAsync(
            string? catalogPattern, string? schemaPattern, string? tableNamePattern, IReadOnlyList<string>? tableTypes, CancellationToken cancellationToken)
        {
            TGetTablesResp getTablesResp = await GetTablesAsync(
                catalogPattern, schemaPattern, tableNamePattern,
                tableTypes?.ToList(), cancellationToken).ConfigureAwait(false);

            TGetResultSetMetadataResp tableMetadata = await GetResultSetMetadataAsync(getTablesResp, cancellationToken).ConfigureAwait(false);
            IReadOnlyDictionary<string, int> columnMap = GetColumnIndexMap(tableMetadata.Schema.Columns);
            TRowSet rowSet = await GetRowSetAsync(getTablesResp, cancellationToken).ConfigureAwait(false);

            IReadOnlyList<string> catalogList = rowSet.Columns[columnMap[TableCat]].StringVal.Values;
            IReadOnlyList<string> schemaList = rowSet.Columns[columnMap[TableSchem]].StringVal.Values;
            IReadOnlyList<string> tableList = rowSet.Columns[columnMap[TableName]].StringVal.Values;
            IReadOnlyList<string> tableTypeList = rowSet.Columns[columnMap[TableType]].StringVal.Values;

            var result = new List<(string, string, string, string)>();
            for (int i = 0; i < catalogList.Count; i++)
            {
                result.Add((catalogList[i], schemaList[i], tableList[i], tableTypeList[i]));
            }
            return result;
        }

        /// <remarks>
        /// Thrift: column metadata (precision, scale, type name) comes from the server via
        /// SetPrecisionScaleAndTypeName. SEA: computed from type name strings via ColumnMetadataHelper.
        /// </remarks>
        async Task IGetObjectsDataProvider.PopulateColumnInfoAsync(string? catalogPattern, string? schemaPattern,
            string? tablePattern, string? columnPattern,
            Dictionary<string, Dictionary<string, Dictionary<string, TableInfo>>> catalogMap,
            CancellationToken cancellationToken)
        {
            TGetColumnsResp columnsResponse = await GetColumnsAsync(
                catalogPattern, schemaPattern, tablePattern, columnPattern, cancellationToken).ConfigureAwait(false);

            TGetResultSetMetadataResp columnsMetadata = await GetResultSetMetadataAsync(columnsResponse, cancellationToken).ConfigureAwait(false);
            IReadOnlyDictionary<string, int> columnMap = GetColumnIndexMap(columnsMetadata.Schema.Columns);
            TRowSet rowSet = await GetRowSetAsync(columnsResponse, cancellationToken).ConfigureAwait(false);

            ColumnsMetadataColumnNames columnNames = GetColumnsMetadataColumnNames();
            IReadOnlyList<string> catalogList = rowSet.Columns[columnMap[columnNames.TableCatalog]].StringVal.Values;
            IReadOnlyList<string> schemaList = rowSet.Columns[columnMap[columnNames.TableSchema]].StringVal.Values;
            IReadOnlyList<string> tableList = rowSet.Columns[columnMap[columnNames.TableName]].StringVal.Values;
            IReadOnlyList<string> columnNameList = rowSet.Columns[columnMap[columnNames.ColumnName]].StringVal.Values;
            ReadOnlySpan<int> columnTypeList = rowSet.Columns[columnMap[columnNames.DataType]].I32Val.Values.Values;
            IReadOnlyList<string> typeNameList = rowSet.Columns[columnMap[columnNames.TypeName]].StringVal.Values;
            ReadOnlySpan<int> nullableList = rowSet.Columns[columnMap[columnNames.Nullable]].I32Val.Values.Values;
            IReadOnlyList<string> columnDefaultList = rowSet.Columns[columnMap[columnNames.ColumnDef]].StringVal.Values;
            ReadOnlySpan<int> ordinalPosList = rowSet.Columns[columnMap[columnNames.OrdinalPosition]].I32Val.Values.Values;
            IReadOnlyList<string> isNullableList = rowSet.Columns[columnMap[columnNames.IsNullable]].StringVal.Values;
            IReadOnlyList<string> isAutoIncrementList = rowSet.Columns[columnMap[columnNames.IsAutoIncrement]].StringVal.Values;
            ReadOnlySpan<int> columnSizeList = rowSet.Columns[columnMap[columnNames.ColumnSize]].I32Val.Values.Values;
            ReadOnlySpan<int> decimalDigitsList = rowSet.Columns[columnMap[columnNames.DecimalDigits]].I32Val.Values.Values;

            for (int i = 0; i < catalogList.Count; i++)
            {
                string catalog = catalogList[i] ?? string.Empty;
                string schemaDb = schemaList[i];
                string tableName = tableList[i];
                string columnName = columnNameList[i];
                short colType = (short)columnTypeList[i];
                string typeName = typeNameList[i];
                short nullable = (short)nullableList[i];
                string? isAutoIncrementString = isAutoIncrementList[i];
                bool isAutoIncrement = !string.IsNullOrEmpty(isAutoIncrementString) && (isAutoIncrementString.Equals("YES", StringComparison.InvariantCultureIgnoreCase) || isAutoIncrementString.Equals("TRUE", StringComparison.InvariantCultureIgnoreCase));
                string isNullable = isNullableList[i] ?? "YES";
                string columnDefault = columnDefaultList[i] ?? "";
                int ordinalPos = ordinalPosList[i] + PositionRequiredOffset;
                int columnSize = columnSizeList[i];
                int decimalDigits = decimalDigitsList[i];
                TableInfo? tableInfo = catalogMap.GetValueOrDefault(catalog)?.GetValueOrDefault(schemaDb)?.GetValueOrDefault(tableName);
                tableInfo?.ColumnName.Add(columnName);
                tableInfo?.ColType.Add(colType);
                tableInfo?.Nullable.Add(nullable);
                tableInfo?.IsAutoIncrement.Add(isAutoIncrement);
                tableInfo?.IsNullable.Add(isNullable);
                tableInfo?.ColumnDefault.Add(columnDefault);
                tableInfo?.OrdinalPosition.Add(ordinalPos);
                SetPrecisionScaleAndTypeName(colType, typeName, tableInfo, columnSize, decimalDigits);
            }
        }

        public override IArrowArrayStream GetTableTypes()
        {
            return this.TraceActivity(activity =>
            {
                TGetTableTypesReq req = new()
                {
                    SessionHandle = SessionHandle ?? throw new InvalidOperationException("session not created"),
                };
                TrySetGetDirectResults(req);

                CancellationToken cancellationToken = ApacheUtility.GetCancellationToken(QueryTimeoutSeconds, ApacheUtility.TimeUnit.Seconds);
                try
                {
                    TGetTableTypesResp resp = Client.GetTableTypes(req, cancellationToken).Result;
                    HandleThriftResponse(resp.Status, activity);

                    TRowSet rowSet = GetRowSetAsync(resp, cancellationToken).Result;
                    StringArray tableTypes = rowSet.Columns[0].StringVal.Values;

                    HashSet<string> distinctTableTypes = new HashSet<string>(tableTypes);

                    StringArray.Builder tableTypesBuilder = new StringArray.Builder();
                    tableTypesBuilder.AppendRange(distinctTableTypes);

                    IArrowArray[] dataArrays = new IArrowArray[]
                    {
                tableTypesBuilder.Build()
                    };

                    return new HiveInfoArrowStream(StandardSchemas.TableTypesSchema, dataArrays);
                }
                catch (Exception ex) when (ExceptionHelper.IsOperationCanceledOrCancellationRequested(ex, cancellationToken))
                {
                    throw new TimeoutException("The metadata query execution timed out. Consider increasing the query timeout value.", ex);
                }
                catch (Exception ex) when (ex is not HiveServer2Exception)
                {
                    throw new HiveServer2Exception($"An unexpected error occurred while running metadata query. '{ApacheUtility.FormatExceptionMessage(ex)}'", ex);
                }
            }, ClassName + "." + nameof(GetTableTypes));
        }

        internal async Task PollForResponseAsync(TOperationHandle operationHandle, TCLIService.IAsync client, int pollTimeMilliseconds, CancellationToken cancellationToken = default)
        {
            await this.TraceActivityAsync(async activity =>
            {
                activity?.AddEvent("hive2.thrift.poll_start");
                TGetOperationStatusResp? statusResponse = null;
                int attempts = 0;
                do
                {
                    if (statusResponse != null) { await Task.Delay(pollTimeMilliseconds, cancellationToken); }
                    TGetOperationStatusReq request = new(operationHandle);
                    attempts++;
                    statusResponse = await client.GetOperationStatus(request, cancellationToken);
                } while (statusResponse.OperationState == TOperationState.PENDING_STATE || statusResponse.OperationState == TOperationState.RUNNING_STATE);
                activity?.AddEvent("hive2.thrift.poll_end",
                [
                    new("hive2.thrift.poll_attempts", attempts),
                    new("hive2.thrift.operation_state", statusResponse.OperationState.ToString()),
                ]);

                // Must be in the finished state to be valid. If not, typically a server error or timeout has occurred.
                if (statusResponse.OperationState != TOperationState.FINISHED_STATE)
                {
#pragma warning disable CS0618 // Type or member is obsolete
                    throw new HiveServer2Exception(statusResponse.ErrorMessage, AdbcStatusCode.InvalidState)
                        .SetSqlState(statusResponse.SqlState)
                        .SetNativeError(statusResponse.ErrorCode);
#pragma warning restore CS0618 // Type or member is obsolete
                }
            }, ClassName + "." + nameof(PollForResponseAsync));
        }

        private string GetInfoTypeStringValue(TGetInfoType infoType)
        {
            return this.TraceActivity(activity =>
            {
                TGetInfoReq req = new()
                {
                    SessionHandle = SessionHandle ?? throw new InvalidOperationException("session not created"),
                    InfoType = infoType,
                };

                CancellationToken cancellationToken = ApacheUtility.GetCancellationToken(QueryTimeoutSeconds, ApacheUtility.TimeUnit.Seconds);
                try
                {
                    TGetInfoResp getInfoResp = Client.GetInfo(req, cancellationToken).Result;
                    HandleThriftResponse(getInfoResp.Status, activity);

                    return getInfoResp.InfoValue.StringValue;
                }
                catch (Exception ex) when (ExceptionHelper.IsOperationCanceledOrCancellationRequested(ex, cancellationToken))
                {
                    throw new TimeoutException("The metadata query execution timed out. Consider increasing the query timeout value.", ex);
                }
                catch (Exception ex) when (ex is not HiveServer2Exception)
                {
                    throw new HiveServer2Exception($"An unexpected error occurred while running metadata query. '{ApacheUtility.FormatExceptionMessage(ex)}'", ex);
                }
            }, nameof(HiveServer2Connection) + "." + nameof(GetInfoTypeStringValue));
        }

        protected override void Dispose(bool disposing)
        {
            if (!_isDisposed && disposing)
            {
                DisposeClient();
                _fileActivityListener?.Dispose();
                _isDisposed = true;
            }
            base.Dispose(disposing);
        }

        private void DisposeClient()
        {
            this.TraceActivity(activity =>
            {
                if (_client != null && SessionHandle != null)
                {
                    CancellationToken cancellationToken = ApacheUtility.GetCancellationToken(QueryTimeoutSeconds, ApacheUtility.TimeUnit.Seconds);

                    TCloseSessionReq r6 = new(SessionHandle);
                    var resp = _client.CloseSession(r6, cancellationToken).Result;
                    HandleThriftResponse(resp.Status, activity);

                    _transport?.Close();
                    if (_client is IDisposable disposableClient)
                    {
                        disposableClient.Dispose();
                    }
                    _transport = null;
                    _client = null;
                }
            }, ClassName + "." + nameof(DisposeClient));
        }

        internal async Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(TOperationHandle operationHandle, TCLIService.IAsync client, CancellationToken cancellationToken = default)
        {
            TGetResultSetMetadataReq request = new(operationHandle);
            TGetResultSetMetadataResp response = await client.GetResultSetMetadata(request, cancellationToken);
            return response;
        }

        /// <summary>
        /// Gets the data-source specific columns names for the GetColumns metadata result.
        /// </summary>
        /// <returns></returns>
        protected abstract ColumnsMetadataColumnNames GetColumnsMetadataColumnNames();

        /// <summary>
        /// Gets the default product version
        /// </summary>
        /// <returns></returns>
        protected abstract string GetProductVersionDefault();

        /// <summary>
        /// Gets the current product version.
        /// </summary>
        /// <returns></returns>
        protected internal string GetProductVersion()
        {
            FileVersionInfo fileVersionInfo = FileVersionInfo.GetVersionInfo(Assembly.GetExecutingAssembly().Location);
            return fileVersionInfo.ProductVersion ?? GetProductVersionDefault();
        }

        protected internal static Uri GetBaseAddress(string? uri, string? hostName, string? path, string? port, string hostOptionName, bool isTlsEnabled)
        {
            // Uri property takes precedent.
            if (!string.IsNullOrWhiteSpace(uri))
            {
                if (!string.IsNullOrWhiteSpace(hostName))
                {
                    throw new ArgumentOutOfRangeException(
                        AdbcOptions.Uri,
                        hostOptionName,
                        $"Conflicting server arguments. Please provide only one of the following options: '{AdbcOptions.Uri}' or '{hostOptionName}'.");
                }

                var uriValue = new Uri(uri);
                if (uriValue.Scheme != Uri.UriSchemeHttp && uriValue.Scheme != Uri.UriSchemeHttps)
                    throw new ArgumentOutOfRangeException(
                        AdbcOptions.Uri,
                        uri,
                        $"Unsupported scheme '{uriValue.Scheme}'");
                return uriValue;
            }

            bool isPortSet = !string.IsNullOrEmpty(port);
            bool isValidPortNumber = int.TryParse(port, out int portNumber) && portNumber > 0;
            string uriScheme = isTlsEnabled ? Uri.UriSchemeHttps : Uri.UriSchemeHttp;
            int uriPort;
            if (!isPortSet)
                uriPort = -1;
            else if (isValidPortNumber)
                uriPort = portNumber;
            else
                throw new ArgumentOutOfRangeException(nameof(port), portNumber, $"Port number is not in a valid range.");

            // UriBuilder percent-encodes '?' as '%3F' when a query string is embedded
            // in the path argument (e.g. /sql/1.0/warehouses/<id>?o=<orgId>).
            // Use the overload that takes extraValue so the query is passed correctly.
            int queryIndex = path?.IndexOf('?') ?? -1;
            Uri baseAddress = new UriBuilder(
                uriScheme,
                hostName,
                uriPort,
                queryIndex >= 0 ? path!.Substring(0, queryIndex) : path,
                queryIndex >= 0 ? path!.Substring(queryIndex) : string.Empty).Uri;
            return baseAddress;
        }

        internal IReadOnlyDictionary<string, int> GetColumnIndexMap(List<TColumnDesc> columns) => columns
           .Select(t => new { Index = t.Position - ColumnMapIndexOffset, t.ColumnName })
           .ToDictionary(t => t.ColumnName, t => t.Index);

        protected abstract int ColumnMapIndexOffset { get; }

        protected abstract Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(IResponse response, CancellationToken cancellationToken = default);

        protected abstract Task<TRowSet> GetRowSetAsync(IResponse response, CancellationToken cancellationToken = default);

        protected internal virtual bool TrySetGetDirectResults(IRequest request) => false;

        protected internal virtual bool TryGetDirectResults(TSparkDirectResults? directResults, [MaybeNullWhen(false)] out QueryResult result)
        {
            result = null;
            return false;
        }

        protected internal virtual bool TryGetDirectResults(
            TSparkDirectResults? directResults,
            [MaybeNullWhen(false)] out TGetResultSetMetadataResp metadata,
            [MaybeNullWhen(false)] out TRowSet rowSet)
        {
            metadata = null;
            rowSet = null;
            return false;
        }

        protected internal abstract int PositionRequiredOffset { get; }

        protected abstract string InfoDriverName { get; }

        protected abstract string InfoDriverArrowVersion { get; }

        protected abstract string ProductVersion { get; }

        protected abstract bool GetObjectsPatternsRequireLowerCase { get; }

        protected abstract bool IsColumnSizeValidForDecimal { get; }

        public override void SetOption(string key, string? value)
        {
            // These option can be set even if already connected.
            switch (key.ToLowerInvariant())
            {
                case AdbcOptions.Telemetry.TraceParent:
                    SetTraceParent(string.IsNullOrWhiteSpace(value) ? null : value);
                    return;
            }

            if (SessionHandle != null)
            {
                throw new AdbcException($"Option '{key}' cannot be set once the connection is open.", AdbcStatusCode.InvalidState);
            }

            // These option can only be set before connection is open.
            switch (key.ToLowerInvariant())
            {
                default:
                    throw AdbcException.NotImplemented($"Option '{key}' is not implemented");
            }
        }

        private static string PatternToRegEx(string? pattern)
        {
            if (pattern == null)
                return ".*";

            StringBuilder builder = new StringBuilder("(?i)^");
            string convertedPattern = pattern.Replace("_", ".").Replace("%", ".*");
            builder.Append(convertedPattern);
            builder.Append('$');

            return builder.ToString();
        }


        internal async Task<TGetCatalogsResp> GetCatalogsAsync(CancellationToken cancellationToken)
        {
            return await this.TraceActivityAsync(async activity =>
            {
                if (SessionHandle == null)
                {
                    throw new InvalidOperationException("Invalid session");
                }

                TGetCatalogsReq req = new TGetCatalogsReq(SessionHandle);
                TrySetGetDirectResults(req);

                TGetCatalogsResp resp = await Client.GetCatalogs(req, cancellationToken);
                HandleThriftResponse(resp.Status, activity);

                return resp;
            }, ClassName + "." + nameof(GetCatalogsAsync));
        }

        internal async Task<TGetSchemasResp> GetSchemasAsync(
            string? catalogName,
            string? schemaName,
            CancellationToken cancellationToken)
        {
            return await this.TraceActivityAsync(async activity =>
            {
                if (SessionHandle == null)
                {
                    throw new InvalidOperationException("Invalid session");
                }

                TGetSchemasReq req = new(SessionHandle);
                TrySetGetDirectResults(req);
                if (catalogName != null)
                {
                    req.CatalogName = catalogName;
                }
                if (schemaName != null)
                {
                    req.SchemaName = schemaName;
                }

                TGetSchemasResp resp = await Client.GetSchemas(req, cancellationToken);
                HandleThriftResponse(resp.Status, activity);

                return resp;
            }, ClassName + "." + nameof(GetSchemasAsync));
        }

        internal async Task<TGetTablesResp> GetTablesAsync(
            string? catalogName,
            string? schemaName,
            string? tableName,
            List<string>? tableTypes,
            CancellationToken cancellationToken)
        {
            return await this.TraceActivityAsync(async activity =>
            {
                if (SessionHandle == null)
                {
                    throw new InvalidOperationException("Invalid session");
                }

                TGetTablesReq req = new(SessionHandle);
                TrySetGetDirectResults(req);
                if (catalogName != null)
                {
                    req.CatalogName = catalogName;
                }
                if (schemaName != null)
                {
                    req.SchemaName = schemaName;
                }
                if (tableName != null)
                {
                    req.TableName = tableName;
                }
                if (tableTypes != null && tableTypes.Count > 0)
                {
                    req.TableTypes = tableTypes;
                }

                TGetTablesResp resp = await Client.GetTables(req, cancellationToken);
                HandleThriftResponse(resp.Status, activity);

                return resp;
            }, ClassName + "." + nameof(GetTablesAsync));
        }

        internal async Task<TGetColumnsResp> GetColumnsAsync(
            string? catalogName,
            string? schemaName,
            string? tableName,
            string? columnName,
            CancellationToken cancellationToken)
        {
            return await this.TraceActivityAsync(async activity =>
            {
                if (SessionHandle == null)
                {
                    throw new InvalidOperationException("Invalid session");
                }

                TGetColumnsReq req = new(SessionHandle);
                TrySetGetDirectResults(req);
                if (catalogName != null)
                {
                    req.CatalogName = catalogName;
                }
                if (schemaName != null)
                {
                    req.SchemaName = schemaName;
                }
                if (tableName != null)
                {
                    req.TableName = tableName;
                }
                if (columnName != null)
                {
                    req.ColumnName = columnName;
                }

                TGetColumnsResp resp = await Client.GetColumns(req, cancellationToken);
                HandleThriftResponse(resp.Status, activity);

                return resp;
            }, ClassName + "." + nameof(GetColumnsAsync));
        }

        internal async Task<TGetPrimaryKeysResp> GetPrimaryKeysAsync(
            string? catalogName,
            string? schemaName,
            string? tableName,
            CancellationToken cancellationToken = default)
        {
            return await this.TraceActivityAsync(async activity =>
            {
                if (SessionHandle == null)
                {
                    throw new InvalidOperationException("Invalid session");
                }

                TGetPrimaryKeysReq req = new(SessionHandle);
                TrySetGetDirectResults(req);
                if (catalogName != null)
                {
                    req.CatalogName = catalogName!;
                }
                if (schemaName != null)
                {
                    req.SchemaName = schemaName!;
                }
                if (tableName != null)
                {
                    req.TableName = tableName!;
                }

                TGetPrimaryKeysResp resp = await Client.GetPrimaryKeys(req, cancellationToken);
                HandleThriftResponse(resp.Status, activity);

                return resp;
            }, ClassName + "." + nameof(GetPrimaryKeysAsync));
        }

        internal async Task<TGetCrossReferenceResp> GetCrossReferenceAsync(
            string? catalogName,
            string? schemaName,
            string? tableName,
            string? foreignCatalogName,
            string? foreignSchemaName,
            string? foreignTableName,
            CancellationToken cancellationToken = default)
        {
            return await this.TraceActivityAsync(async activity =>
            {
                if (SessionHandle == null)
                {
                    throw new InvalidOperationException("Invalid session");
                }

                TGetCrossReferenceReq req = new(SessionHandle);
                TrySetGetDirectResults(req);
                if (catalogName != null)
                {
                    req.ParentCatalogName = catalogName!;
                }
                if (schemaName != null)
                {
                    req.ParentSchemaName = schemaName!;
                }
                if (tableName != null)
                {
                    req.ParentTableName = tableName!;
                }
                if (foreignCatalogName != null)
                {
                    req.ForeignCatalogName = foreignCatalogName!;
                }
                if (foreignSchemaName != null)
                {
                    req.ForeignSchemaName = foreignSchemaName!;
                }
                if (foreignTableName != null)
                {
                    req.ForeignTableName = foreignTableName!;
                }

                TGetCrossReferenceResp resp = await Client.GetCrossReference(req, cancellationToken);
                HandleThriftResponse(resp.Status, activity);
                return resp;
            }, ClassName + "." + nameof(GetCrossReferenceAsync));
        }

        internal abstract void SetPrecisionScaleAndTypeName(short columnType, string typeName, TableInfo? tableInfo, int columnSize, int decimalDigits);

        public override Schema GetTableSchema(string? catalog, string? dbSchema, string? tableName)
        {
            return this.TraceActivity(activity =>
            {
                if (SessionHandle == null)
                {
                    throw new InvalidOperationException("Invalid session");
                }

                TGetColumnsReq getColumnsReq = new TGetColumnsReq(SessionHandle);
                getColumnsReq.CatalogName = catalog;
                getColumnsReq.SchemaName = dbSchema;
                getColumnsReq.TableName = tableName;
                TrySetGetDirectResults(getColumnsReq);

                CancellationToken cancellationToken = ApacheUtility.GetCancellationToken(QueryTimeoutSeconds, ApacheUtility.TimeUnit.Seconds);
                try
                {
                    var columnsResponse = Client.GetColumns(getColumnsReq, cancellationToken).Result;
                    HandleThriftResponse(columnsResponse.Status, activity);

                    TRowSet rowSet = GetRowSetAsync(columnsResponse, cancellationToken).Result;
                    List<TColumn> columns = rowSet.Columns;
                    int rowCount = rowSet.Columns[3].StringVal.Values.Length;

                    Field[] fields = new Field[rowCount];
                    for (int i = 0; i < rowCount; i++)
                    {
                        string columnName = columns[3].StringVal.Values.GetString(i);
                        int? columnType = columns[4].I32Val.Values.GetValue(i);
                        string typeName = columns[5].StringVal.Values.GetString(i);
                        // Note: the following two columns do not seem to be set correctly for DECIMAL types.
                        bool isColumnSizeValid = IsColumnSizeValidForDecimal;
                        int? columnSize = columns[6].I32Val.Values.GetValue(i);
                        int? decimalDigits = columns[8].I32Val.Values.GetValue(i);
                        bool nullable = columns[10].I32Val.Values.GetValue(i) == 1;
                        IArrowType dataType = GetArrowType(columnType!.Value, typeName, isColumnSizeValid, columnSize, decimalDigits);
                        fields[i] = new Field(columnName, dataType, nullable);
                    }
                    return new Schema(fields, null);
                }
                catch (Exception ex) when (ExceptionHelper.IsOperationCanceledOrCancellationRequested(ex, cancellationToken))
                {
                    throw new TimeoutException("The metadata query execution timed out. Consider increasing the query timeout value.", ex);
                }
                catch (Exception ex) when (ex is not HiveServer2Exception)
                {
                    throw new HiveServer2Exception($"An unexpected error occurred while running metadata query. '{ApacheUtility.FormatExceptionMessage(ex)}'", ex);
                }
            }, ClassName + "." + nameof(GetTableSchema));
        }

        /// <summary>
        /// Converts a column type ID to an Apache Arrow type.
        /// For DECIMAL types, uses server-provided precision/scale when available,
        /// otherwise parses from the type name string.
        /// </summary>
        /// <exception cref="NotImplementedException">Thrown for unsupported column type IDs.</exception>
        internal static IArrowType GetArrowType(int columnTypeId, string typeName, bool isColumnSizeValid, int? columnSize, int? decimalDigits)
        {
            switch (columnTypeId)
            {
                case (int)ColumnTypeId.BOOLEAN:
                    return BooleanType.Default;
                case (int)ColumnTypeId.TINYINT:
                    return Int8Type.Default;
                case (int)ColumnTypeId.SMALLINT:
                    return Int16Type.Default;
                case (int)ColumnTypeId.INTEGER:
                    return Int32Type.Default;
                case (int)ColumnTypeId.BIGINT:
                    return Int64Type.Default;
                case (int)ColumnTypeId.FLOAT:
                case (int)ColumnTypeId.REAL:
                    return FloatType.Default;
                case (int)ColumnTypeId.DOUBLE:
                    return DoubleType.Default;
                case (int)ColumnTypeId.VARCHAR:
                case (int)ColumnTypeId.NVARCHAR:
                case (int)ColumnTypeId.LONGVARCHAR:
                case (int)ColumnTypeId.LONGNVARCHAR:
                    return StringType.Default;
                case (int)ColumnTypeId.TIMESTAMP:
                    return new TimestampType(TimeUnit.Microsecond, timezone: (string?)null);
                case (int)ColumnTypeId.BINARY:
                case (int)ColumnTypeId.VARBINARY:
                case (int)ColumnTypeId.LONGVARBINARY:
                    return BinaryType.Default;
                case (int)ColumnTypeId.DATE:
                    return Date32Type.Default;
                case (int)ColumnTypeId.CHAR:
                case (int)ColumnTypeId.NCHAR:
                    return StringType.Default;
                case (int)ColumnTypeId.DECIMAL:
                case (int)ColumnTypeId.NUMERIC:
                    if (isColumnSizeValid && columnSize.HasValue && decimalDigits.HasValue)
                    {
                        return new Decimal128Type(columnSize.Value, decimalDigits.Value);
                    }
                    else
                    {
                        // Note: parsing the type name for SQL DECIMAL types as the precision and scale values
                        // may not be returned in the Thrift call to GetColumns
                        return SqlTypeNameParser<SqlDecimalParserResult>
                            .Parse(typeName, columnTypeId)
                            .Decimal128Type;
                    }
                case (int)ColumnTypeId.NULL:
                    return NullType.Default;
                case (int)ColumnTypeId.ARRAY:
                case (int)ColumnTypeId.JAVA_OBJECT:
                case (int)ColumnTypeId.STRUCT:
                    return StringType.Default;
                default:
                    throw new NotImplementedException($"Column type id: {columnTypeId} is not supported.");
            }
        }

        internal async Task<TRowSet> FetchResultsAsync(TOperationHandle operationHandle, long batchSize = BatchSizeDefault, CancellationToken cancellationToken = default)
        {
            return await this.TraceActivityAsync(async activity =>
            {
                await PollForResponseAsync(operationHandle, Client, PollTimeMillisecondsDefault, cancellationToken);

                TFetchResultsResp fetchResp = await FetchNextAsync(operationHandle, Client, batchSize, cancellationToken);
                if (fetchResp.Status.StatusCode == TStatusCode.ERROR_STATUS)
                {
                    throw new HiveServer2Exception(fetchResp.Status.ErrorMessage)
                        .SetNativeError(fetchResp.Status.ErrorCode)
                        .SetSqlState(fetchResp.Status.SqlState);
                }
                activity?.AddTag(SemanticConventions.Db.Response.ReturnedRows, HiveServer2Reader.GetRowCount(fetchResp.Results, fetchResp.Results.Columns.Count));
                return fetchResp.Results;
            }, ClassName + "." + nameof(FetchResultsAsync));
        }

        private static async Task<TFetchResultsResp> FetchNextAsync(TOperationHandle operationHandle, TCLIService.IAsync client, long batchSize, CancellationToken cancellationToken = default)
        {
            TFetchResultsReq request = new(operationHandle, TFetchOrientation.FETCH_NEXT, batchSize);
            TFetchResultsResp response = await client.FetchResults(request, cancellationToken);
            return response;
        }

        public override IArrowArrayStream GetInfo(IReadOnlyList<AdbcInfoCode> codes)
        {
            return this.TraceActivity(activity =>
            {
                if (codes.Count == 0)
                    codes = infoSupportedCodes;

                var values = new Dictionary<AdbcInfoCode, object>();
                foreach (var code in codes)
                {
                    object? val = code switch
                    {
                        AdbcInfoCode.DriverName => InfoDriverName,
                        AdbcInfoCode.DriverVersion => ProductVersion,
                        AdbcInfoCode.DriverArrowVersion => InfoDriverArrowVersion,
                        AdbcInfoCode.VendorName => VendorName,
                        AdbcInfoCode.VendorVersion => VendorVersion ?? "",
                        AdbcInfoCode.VendorSql => InfoVendorSql,
                        _ => null
                    };
                    if (val != null) values[code] = val;

                    string tagKey = SemanticConventions.Db.Operation.Parameter(code.ToString().ToLowerInvariant());
                    ActivityExtensions.AddTag(activity, tagKey, () => val);
                }

                return MetadataSchemaFactory.BuildGetInfoResult(codes, values);
            }, ClassName + "." + nameof(GetInfo));
        }

        private static class ExceptionHelper
        {
            public static bool IsOperationCanceledOrCancellationRequested(Exception ex, CancellationToken cancellationToken)
            {
                return ApacheUtility.ContainsException(ex, out OperationCanceledException? _) ||
                       ApacheUtility.ContainsException(ex, out TTransportException? _) && cancellationToken.IsCancellationRequested;
            }
        }

        internal static void HandleThriftResponse(TStatus status, Activity? activity)
        {
            if (ErrorHandlers.TryGetValue(status.StatusCode, out Action<TStatus, Activity?>? handler))
            {
                handler(status, activity);
            }
        }

        private static IReadOnlyDictionary<TStatusCode, Action<TStatus, Activity?>> ErrorHandlers => new Dictionary<TStatusCode, Action<TStatus, Activity?>>()
        {
            [TStatusCode.ERROR_STATUS] = (status, _) => ThrowErrorResponse(status),
            [TStatusCode.INVALID_HANDLE_STATUS] = (status, _) => ThrowErrorResponse(status),
            [TStatusCode.STILL_EXECUTING_STATUS] = (status, _) => ThrowErrorResponse(status, AdbcStatusCode.InvalidState),
            [TStatusCode.SUCCESS_STATUS] = (status, activity) => activity?.AddTag(SemanticConventions.Db.Response.StatusCode, (int)status.StatusCode),
            [TStatusCode.SUCCESS_WITH_INFO_STATUS] = (status, activity) =>
            {
                activity?.AddTag(SemanticConventions.Db.Response.StatusCode, (int)status.StatusCode);
                activity?.AddTag(SemanticConventions.Db.Response.InfoMessages, string.Join(Environment.NewLine, status.InfoMessages));
            },
        };

        private static void ThrowErrorResponse(TStatus status, AdbcStatusCode adbcStatusCode = AdbcStatusCode.InternalError) =>
            throw new HiveServer2Exception(status.ErrorMessage, adbcStatusCode)
                .SetSqlState(status.SqlState)
                .SetNativeError(status.ErrorCode);

        protected TConfiguration GetTconfiguration()
        {
            var thriftConfig = new TConfiguration();
            Activity? activity = Activity.Current;

            Properties.TryGetValue(ThriftTransportSizeConstants.MaxMessageSize, out string? maxMessageSize);
            if (int.TryParse(maxMessageSize, out int maxMessageSizeValue) && maxMessageSizeValue > 0)
            {
                activity?.AddTag(ActivityKeys.Thrift.MaxMessageSize, maxMessageSizeValue);
                thriftConfig.MaxMessageSize = maxMessageSizeValue;
            }

            Properties.TryGetValue(ThriftTransportSizeConstants.MaxFrameSize, out string? maxFrameSize);
            if (int.TryParse(maxFrameSize, out int maxFrameSizeValue) && maxFrameSizeValue > 0)
            {
                activity?.AddTag(ActivityKeys.Thrift.MaxFrameSize, maxFrameSizeValue);
                thriftConfig.MaxFrameSize = maxFrameSizeValue;
            }
            return thriftConfig;
        }

        protected static class ActivityKeys
        {
            private const string Prefix = "hive2.";

            public const string Encrypted = Prefix + HiveServer2.ActivityKeys.Encrypted;
            public const string TransportType = Prefix + HiveServer2.ActivityKeys.TransportType;
            public const string AuthType = Prefix + HiveServer2.ActivityKeys.AuthType;

            public static class Http
            {
                public const string AuthScheme = Prefix + HiveServer2.ActivityKeys.Http.AuthScheme;
                public const string UserAgent = Prefix + HiveServer2.ActivityKeys.Http.UserAgent;
                public const string Uri = Prefix + HiveServer2.ActivityKeys.Http.Uri;
            }

            public static class Thrift
            {
                public const string MaxMessageSize = Prefix + HiveServer2.ActivityKeys.Thrift.MaxMessageSize;
                public const string MaxFrameSize = Prefix + HiveServer2.ActivityKeys.Thrift.MaxFrameSize;
            }
        }
    }
}
