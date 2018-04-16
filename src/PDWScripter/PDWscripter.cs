// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.SqlClient;
using Newtonsoft.Json;
using System.Text.RegularExpressions;

namespace DWScripter
{
   public class PDWscripter
    {
        public SqlConnection conn;
        private SqlCommand cmd;
        private SqlDataReader rdr;
        public List<TableDef> dbTables;
        public string DatabaseName;
        private List<ColumnDef> cols;
        public DBStruct dbstruct;
        private string distColumn;
        private List<IndexColumnDef> clusteredCols;
        private List<PartitionBoundary> partitionBoundaries;
        private string createTableTxt;
        private string alterTableTxt;
        private string dropTableTxt;
        private string dropDeployTmpTableTxt;
        private string dropDeployTableTxt;
        private string copyDataToTmpTableTxt;
        private string copyDataFromTmpTableTxt;
        private string createSchemaTxt;
        private string dropSchemaTxt;
        private string createDeployTmpSchemaTxt;
        private string dropDeployTmpSchemaTxt;
        private string sourceDb;
        private string destDb;
        private string sourceTable;
        private string sourceTmpTableName;
        private string destTable;
        private string destTableFullName;
        private Int16 distribution_policy;
        private string columnClause;
        private string columnSelect;
        private string clusteredClause;
        private string partitionBoundaryClause;
        private string partitionColumn;
        private string partitionLeftOrRight;
        private string filterSpec;
        public string ExcludeObjectSuffixList;
        private string CommandTimeout;
        private string wrkMode;
        private string scriptMode;
        private List<NonclusteredIndexDef> nonclusteredIndexes;
        private string nonClusteredClause;
        private List<StatDef> stats;
        private string statsClause;
        private string warningFile;
        public List<KeyValuePair<String, String>> DbObjectDefinitions;


        public PDWscripter()
        {
            cols = new List<ColumnDef>();
            clusteredCols = new List<IndexColumnDef>();
            partitionBoundaries = new List<PartitionBoundary>();
            nonclusteredIndexes = new List<NonclusteredIndexDef>();
            stats = new List<StatDef>();
            dbTables = new List<TableDef>();
            dbstruct = new DBStruct();
        }
        public PDWscripter(string system, string server, string sourceDb, string authentication, string userName, string pwd, string wrkMode, string ExcludeObjectSuffixList, string filterSpec, string scriptMode, string CommandTimeout)
        {
            DatabaseName = sourceDb;
            cols = new List<ColumnDef>();
            clusteredCols = new List<IndexColumnDef>();
            partitionBoundaries = new List<PartitionBoundary>();
            nonclusteredIndexes = new List<NonclusteredIndexDef>();
            stats = new List<StatDef>();
            dbTables = new List<TableDef>();
            dbstruct = new DBStruct();
            this.filterSpec = filterSpec;
            this.ExcludeObjectSuffixList = ExcludeObjectSuffixList;
            this.wrkMode = wrkMode;
            this.sourceDb = sourceDb;
            this.destDb = sourceDb;         // For future DB cloning 
            this.scriptMode = scriptMode;
            this.CommandTimeout = CommandTimeout;

            SqlConnectionStringBuilder constrbuilder = new SqlConnectionStringBuilder();

            conn = new System.Data.SqlClient.SqlConnection();
            if (system == "PDW")
            {
                constrbuilder.DataSource = server;
                constrbuilder.InitialCatalog = sourceDb;
                if (authentication == "SQL")
                {
                    constrbuilder.UserID = userName;
                    constrbuilder.Password = pwd;
                    constrbuilder.IntegratedSecurity = false;

                }
                else
                {
                    constrbuilder.IntegratedSecurity = true;
                }
                conn.ConnectionString = constrbuilder.ConnectionString;
                
            }
            else
            {
                conn.ConnectionString = "server=" + server + ";database=" + sourceDb + ";User ID=" + userName + ";Password=" + pwd;
            }


            cmd = new System.Data.SqlClient.SqlCommand();
            //sets non default timeout
            if(!String.IsNullOrEmpty(this.CommandTimeout))
            {
                cmd.CommandTimeout = Convert.ToInt32(this.CommandTimeout);
                
            }
            Console.WriteLine("Current Command Timeout: " + cmd.CommandTimeout);

            try {
                conn.Open();
                cmd.Connection = conn;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                throw ex;
            }
            
        }

        private void getSchemas(StreamWriter sw, Boolean GetStructure)
        {
            cmd.CommandText =
                "select name from sys.schemas where name not in ('dbo','sys','INFORMATION_SCHEMA')";

            rdr = cmd.ExecuteReader();

            while (rdr.Read())
            {
                if (!GetStructure)
                {
                    createSchemaTxt = "CREATE SCHEMA [" + rdr.GetString(rdr.GetOrdinal("name")) + "];\r\nGO\r\n";
                    if (sw != null)
                        sw.WriteLine(createSchemaTxt);
                }
                else
                    dbstruct.schemas.Add(rdr.GetString(rdr.GetOrdinal("name")));
            }

            rdr.Close();
        }

        private void CompareSchemas(StreamWriter sw, PDWscripter cSource, PDWscripter cTarget, Boolean SourceFromFile, FilterSettings FilterSet)
        {
            List<string> TargetSchemas = new List<string>();
            List<string> SourceSchemas = new List<string>();
            StringBuilder dropscript = new StringBuilder();
            String createUseDbTxt = string.Empty;

            String strStartWarningMessage = string.Empty;
            String strEndWarningMessage = string.Empty;

            cTarget.cmd.CommandText = "select name from sys.schemas where name not in ('dbo','sys','INFORMATION_SCHEMA')";

            //==> SOURCE
            if (SourceFromFile)
            {
                SourceSchemas.AddRange(dbstruct.schemas);
            }
            else
            {
                cSource.cmd.CommandText = "select name from sys.schemas where name not in ('dbo','sys','INFORMATION_SCHEMA')";
                rdr = cSource.cmd.ExecuteReader();
                while (rdr.Read())
                {
                    SourceSchemas.Add(rdr.GetString(rdr.GetOrdinal("name")));
                }
                rdr.Close();
            }
            //==> TARGET
            rdr = cTarget.cmd.ExecuteReader();
            while (rdr.Read())
            {
                TargetSchemas.Add(rdr.GetString(rdr.GetOrdinal("name")));

            }
            rdr.Close();

            List<string> ListSchemasToCreate;
            List<string> ListSchemasToDelete;
            // COMPARE
            if (FilterSet.Granularity != "None")
            {
                ListSchemasToCreate = (SourceSchemas.Intersect(FilterSet.GetSchemas())).Except(TargetSchemas).ToList();
                ListSchemasToDelete = new List<string>();
              
            }
            else
            {
                ListSchemasToCreate = SourceSchemas.Except(TargetSchemas).ToList();
                ListSchemasToDelete = TargetSchemas.Except(SourceSchemas).ToList();
            }


            string description = "/*####################################################################################################################################################*/\r\n";
            description += "--schemas - to create =" + ListSchemasToCreate.Count.ToString() + " - to delete = " + ListSchemasToDelete.Count.ToString() + "\r\n";
            description += "PRINT 'schemas creation'\r\nGO\r\n";
            sw.WriteLine(description);
            if (ListSchemasToDelete.Count > 0)
                writeWarningtxt(description);

            foreach (string schemaTobeCreated in ListSchemasToCreate)
            {
                createSchemaTxt = "CREATE SCHEMA [" + schemaTobeCreated + "];\r\nGO\r\n";
                sw.WriteLine(createSchemaTxt);
            }

            dropSchemaTxt = string.Empty;

            foreach (string schemaTobeDeleted in ListSchemasToDelete)
            {
                dropSchemaTxt = "/* DROP SCHEMA " + schemaTobeDeleted + ";\r\nGO */\r\n";

                dropscript.Append(dropSchemaTxt);

            }

            if (dropscript.Length > 0)
            {
                createUseDbTxt = "USE " + cTarget.sourceDb + "\r\nGO\r\n";
                strStartWarningMessage = "/* WARNING !!!! ======:  SCHEMAS TO DROP.\r\n";
                strEndWarningMessage = "*/\r\n\r\n";
                dropSchemaTxt = strStartWarningMessage + dropscript.ToString() + strEndWarningMessage;
                sw.WriteLine(dropSchemaTxt);

                dropSchemaTxt = strStartWarningMessage + createUseDbTxt + dropscript.ToString() + strEndWarningMessage;
                writeWarningtxt(dropSchemaTxt);
            }


        }

        private void CompareDbTables(StreamWriter sw, PDWscripter cSource, PDWscripter cTarget, Boolean SourceFromFile, FilterSettings FilterSet)
        {

            if (SourceFromFile)
            {
                cSource.dbTables = new List<TableDef>();
                foreach (TableSt tbl in cSource.dbstruct.tables)
                {
                    cSource.dbTables.Add(new TableDef(tbl.name, tbl.schema, tbl.distribution_policy));
                }

                cTarget.dbTables = new List<TableDef>();
                foreach (TableSt tbl in cTarget.dbstruct.tables)
                {
                    cTarget.dbTables.Add(new TableDef(tbl.name, tbl.schema, tbl.distribution_policy));
                }

            }

            // COMPARE
            List<TableDef> ListdbTablesToCreate;
            List<TableDef> ListTablesToDelete = new List<TableDef>();
            List<TableDef> ListTablesToBeAlter;

            List<TableDef> ListdbTablesSourceFiltered;
            List<TableDef> ListdbTablesTargetFiltered;


            switch (FilterSet.Granularity.ToUpper())
            {
                case "SCHEMA":
                    
                    ListdbTablesSourceFiltered = cSource.dbTables.FindAll(delegate (TableDef tdef) { return FilterSet.GetSchemas().Contains(tdef.schema); });
                    ListdbTablesTargetFiltered = cTarget.dbTables.FindAll(delegate (TableDef tdef) { return FilterSet.GetSchemas().Contains(tdef.schema); });

                    ListdbTablesToCreate = ListdbTablesSourceFiltered.Except(ListdbTablesTargetFiltered).ToList();
                    ListTablesToDelete = ListdbTablesTargetFiltered.Except(ListdbTablesSourceFiltered).ToList();
                    ListTablesToBeAlter = ListdbTablesSourceFiltered.Intersect(ListdbTablesTargetFiltered).ToList();
                    break;

                case "OBJECTS":

                    
                    ListdbTablesSourceFiltered = cSource.dbTables.FindAll(delegate (TableDef Table) { return FilterSet.GetSchemaNameObjects().Contains(Table.name); });
                    ListdbTablesTargetFiltered = cTarget.dbTables.FindAll(delegate (TableDef Table) { return FilterSet.GetSchemaNameObjects().Contains(Table.name); });

                    ListdbTablesToCreate = ListdbTablesSourceFiltered.Except(ListdbTablesTargetFiltered).ToList();
                    ListTablesToBeAlter = ListdbTablesSourceFiltered.Intersect(ListdbTablesTargetFiltered).ToList();
                    
                    break;

                default:
                    ListdbTablesToCreate = cSource.dbTables.Except(cTarget.dbTables).ToList();
                    ListTablesToDelete = cTarget.dbTables.Except(cSource.dbTables).ToList();
                    ListTablesToBeAlter = cSource.dbTables.Intersect(cTarget.dbTables).ToList();
                    break;
            }


            bool bWarning = false;
            string description = "/*####################################################################################################################################################*/\r\n";
            description += "--tables - to create = " + ListdbTablesToCreate.Count.ToString() + " - to delete = " + ListTablesToDelete.Count.ToString() + " - to compare = " + ListTablesToBeAlter.Count.ToString() + "\r\n";
            description += "PRINT 'tables creation'\r\n";
            sw.WriteLine(description);
            if (ListTablesToDelete.Count > 0)
                writeWarningtxt(description);

            // ==> To Delete
            foreach (TableDef t in ListTablesToDelete)
            {
                cTarget.sourceTable = t.name;
                cTarget.destTable = t.name;

                // case distribution change then add to alter list
                if (cSource.dbTables.Exists(x => x.name == t.name))
                {
                    ListTablesToBeAlter.Insert(0, t);
                }
                else
                {
                    
                    bWarning = true;
                    cTarget.compBuildDropTableText(sw, bWarning);
                }
            }

            // ==> To Create
            foreach (TableDef t in ListdbTablesToCreate)
            {
                // case distribution change 
                if (!cTarget.dbTables.Exists(x => x.name == t.name))
                {
                    cSource.sourceTable = t.name;
                    cSource.destTable = t.name;
                    cSource.distribution_policy = t.distribution_policy;
                    cSource.getSourceColumns(false, SourceFromFile);
                    cSource.getClusteredIndex(false, SourceFromFile);
                    cSource.getPartitioning(false, SourceFromFile);
                    cSource.getNonclusteredIndexes(false, SourceFromFile);
                    cSource.getStats(false, SourceFromFile);

                    

                    cSource.buildCreateTableText(sw, cTarget.destDb, false);
                }
            }


            // ==> To ALTER
            foreach (TableDef t in ListTablesToBeAlter)
            {

                cSource.buildAlterTableText(sw, cSource, cTarget, t, SourceFromFile);
            }



        }


        private void writeWarningtxt(string messagewarning)
        {

            StreamWriter sw = null;

            sw = new StreamWriter(warningFile, true);

            sw.WriteLine(messagewarning);

            sw.Close();

        }

        public void getDbstructure(string outFile, string wrkMode, bool generateFile)
        {
            if (wrkMode == "ALL" || wrkMode == "DDL")
            {
                Console.Write("Getting " + this.sourceDb + " database DDL structure");
                string outDBJsonDDLStructureFile = outFile + "_STRUCT_DDL.json";
                StreamWriter sw = null;
                FileStream fs = null;

                getDbTables(true);
                Console.Write(".");
                getSchemas(sw, true);
                Console.Write(".");
                getSourceColumnsGlobal();
                Console.Write(".");
                getClusteredIndexGlobal();
                getNonclusteredIndexesGlobal();
                Console.Write(".");
                getStatsGlobal();
                Console.Write(".");
                getPartitioningGlobal();

                Console.Write("\r\nDone\r\n");
                if (generateFile)
                {

                    Console.Write("PersistStructure DDL to JSON file :" + outDBJsonDDLStructureFile + ">");
                    if (outDBJsonDDLStructureFile != "")
                    {
                        fs = new FileStream(outDBJsonDDLStructureFile, FileMode.Create);
                        sw = new StreamWriter(fs);
                    }
                    sw.Write(JsonConvert.SerializeObject(dbstruct));
                    sw.Close();
                }
            }

            if (wrkMode == "ALL" || wrkMode == "DML")
            {

                Console.Write("Getting " + this.sourceDb + " database DML structure");
                cmd.CommandText = @" SELECT c.definition, b.name + '.' + a.name AS ObjectName
                    FROM
                    sys.sql_modules c
                    INNER JOIN sys.objects a ON a.object_id = c.object_id
                    INNER JOIN sys.schemas b
                    ON a.schema_id = b.schema_id";

                rdr = cmd.ExecuteReader();
                DbObjectDefinitions = new List<KeyValuePair<string, string>>();
                Regex r = new Regex(this.ExcludeObjectSuffixList, RegexOptions.IgnoreCase);
                string ModuleName;
                while (rdr.Read())
                {
                    IDataRecord record = (IDataRecord)rdr;
                    ModuleName = record[1].ToString();
                    if (!r.IsMatch(ModuleName))
                    {

                        KeyValuePair<String, String> kvpObjNameDef = new KeyValuePair<String, String>(String.Format("{0}", record[1]), String.Format("{0}", record[0]).TrimEnd(new char[] { '\r', '\n', ' ' }));

                        if (!DbObjectDefinitions.Exists(objDef => objDef.Key == kvpObjNameDef.Key))
                        {

                            // Object doesn't exist
                            if (!DbObjectDefinitions.Any(objDef => objDef.Value.Contains(kvpObjNameDef.Key)))
                            {
                                // Object never used by an other object
                                DbObjectDefinitions.Add(kvpObjNameDef);
                            }
                            else
                            {
                                // Object already used by an other object, we had it previously to the calling one
                                int idxCallingObj = DbObjectDefinitions.IndexOf(DbObjectDefinitions.First(objDef => objDef.Value.Contains(kvpObjNameDef.Key)));
                                DbObjectDefinitions.Insert(idxCallingObj, kvpObjNameDef);
                            }
                        }
                        Console.Write(".");
                    }
                }
                rdr.Close();
                Console.Write("\r\nDone\r\n");
                if (generateFile)
                {
                    string outDBJsonDMLStructureFile = outFile + "_STRUCT_DML.json";
                    StreamWriter sw = null;
                    FileStream fs = null;
                    Console.Write("PersistStructure DML to JSON file :" + outDBJsonDMLStructureFile + ">");
                    if (outDBJsonDMLStructureFile != "")
                    {
                        fs = new FileStream(outDBJsonDMLStructureFile, FileMode.Create);
                        sw = new StreamWriter(fs);
                    }
                    sw.Write(JsonConvert.SerializeObject(DbObjectDefinitions));
                    sw.Close();
                }
            }
        }
        public void getDbTables(Boolean getStructure)
        {
            dbTables.Clear();
            Regex r = new Regex(this.ExcludeObjectSuffixList, RegexOptions.IgnoreCase);
            string TableName;
            cmd.CommandText =
                "select schema_name(so.schema_id) + '.' + so.name as name, tdp.distribution_policy, schema_name(so.schema_id) as [schema] " +
                "from sys.tables so left join sys.external_tables et on so.object_id = et.object_id " +
                "JOIN sys.pdw_table_distribution_properties AS tdp ON so.object_id = tdp.object_id " +
                "where et.name is NULL and so.type = 'U' " +
                "and so.name like '" + filterSpec + "' " +
                "order by so.name ";

            rdr = cmd.ExecuteReader();

            if (rdr.HasRows)  
            {
                while (rdr.Read())
                {
                    TableName = rdr.GetString(rdr.GetOrdinal("name"));
                    if (!r.IsMatch(TableName))
                    { 
                        if (!getStructure)
                            dbTables.Add(new TableDef(
                                rdr.GetString(rdr.GetOrdinal("name")),
                                rdr.GetString(rdr.GetOrdinal("schema")),
                                rdr.GetByte(rdr.GetOrdinal("distribution_policy"))
                                ));
                        else
                            dbstruct.tables.Add(new TableSt(rdr.GetString(rdr.GetOrdinal("name")), rdr.GetString(rdr.GetOrdinal("schema")),
                            rdr.GetByte(rdr.GetOrdinal("distribution_policy"))
                            ));
                }
                }
            }
            rdr.Close();

        }
        private void getSourceColumnsGlobal()
        {

            List<ColumnDef> columns = new List<ColumnDef>();
            string TableKey = "";
            string SchemaName;
            string TableName;
            string tableKeyPrevious = "";
            TableSt TableStruct = new TableSt();
            cmd.CommandText =
                   @"select  schema_name(tbl.schema_id) as SchemaName,tbl.Name as TableName, c.column_id, c.name, t.name as type, c.max_length, c.precision,
                        c.scale, c.is_nullable, d.distribution_ordinal, c.collation_name, ISNULL('DEFAULT ' + dc.definition, '') as DefaultConstraint
                            from sys.columns c
                                join sys.pdw_column_distribution_properties d
                                on c.object_id = d.object_id and c.column_id = d.column_id
                                    join sys.types t on t.user_type_id = c.user_type_id
                                        left join sys.default_constraints dc on c.default_object_id = dc.object_id and c.object_id = dc.parent_object_id
                                            inner join sys.tables tbl on tbl.object_id = c.object_id and tbl.type = 'U'
                                                order by schema_name(tbl.schema_id),tbl.name, Column_Id ";
            rdr = cmd.ExecuteReader();

            while (rdr.Read())
                {
                    SchemaName = rdr.GetString(rdr.GetOrdinal("SchemaName"));
                    TableName = rdr.GetString(rdr.GetOrdinal("TableName"));
                    TableKey = SchemaName + "." + TableName;
                    if (TableKey != tableKeyPrevious)
                    {
                        if (columns.Count != 0 && (TableStruct != null))
                        {
                            TableStruct.Columns = columns;
                        }
                        TableStruct = this.dbstruct.GetTable(TableKey);


                        columns = new List<ColumnDef>();
                        tableKeyPrevious = TableKey;
                    }

                    columns.Add(new ColumnDef(
                        rdr.GetInt32(rdr.GetOrdinal("column_id")),
                        rdr.GetString(rdr.GetOrdinal("name")),
                        rdr.GetString(rdr.GetOrdinal("type")),
                        rdr.GetInt16(rdr.GetOrdinal("max_length")),
                        rdr.GetByte(rdr.GetOrdinal("precision")),
                        rdr.GetByte(rdr.GetOrdinal("scale")),
                        rdr.GetBoolean(rdr.GetOrdinal("is_nullable")),
                        rdr.GetByte(rdr.GetOrdinal("distribution_ordinal")),
                        rdr.GetString(rdr.GetOrdinal("DefaultConstraint")),
                        rdr["collation_name"] == DBNull.Value ? string.Empty : (string)rdr["collation_name"]
                        ));

                }
            if (columns.Count != 0 && (TableStruct != null))
            {
                TableStruct.Columns = columns;
            }
        
            rdr.Close();


            }
        private void getSourceColumns(Boolean getStruture, Boolean SourceFromFile)
        {
            cols.Clear();
            distColumn = "";
            columnClause = "";
            StringBuilder columnSelect = new StringBuilder();
            StringBuilder columnspec = new StringBuilder();

            List<ColumnDef> tempCols = new List<ColumnDef>();

            if (!SourceFromFile)
            {
                cmd.CommandText =
                    "select c.column_id, c.name, t.name as type, c.max_length, c.precision," +
                    "c.scale, c.is_nullable, d.distribution_ordinal, c.collation_name, ISNULL('DEFAULT '+dc.definition,'') as DefaultConstraint " +
                    "from sys.columns c " +
                    "join sys.pdw_column_distribution_properties d " +
                    "on c.object_id = d.object_id and c.column_id = d.column_id " +
                    "join sys.types t on t.user_type_id = c.user_type_id " +
                    "left join sys.default_constraints dc on c.default_object_id =dc.object_id and c.object_id =dc.parent_object_id " +
                    "where c.object_id = (select object_id from sys.tables where schema_name(schema_id) + '.' + name = '" + sourceTable + "') " +
                    "order by Column_Id ";

                rdr = cmd.ExecuteReader();

                if (rdr.HasRows) 
                {
                    while (rdr.Read())
                    {

                        cols.Add(new ColumnDef(
                            rdr.GetInt32(rdr.GetOrdinal("column_id")),
                            rdr.GetString(rdr.GetOrdinal("name")),
                            rdr.GetString(rdr.GetOrdinal("type")),
                            rdr.GetInt16(rdr.GetOrdinal("max_length")),
                            rdr.GetByte(rdr.GetOrdinal("precision")),
                            rdr.GetByte(rdr.GetOrdinal("scale")),
                            rdr.GetBoolean(rdr.GetOrdinal("is_nullable")),
                            rdr.GetByte(rdr.GetOrdinal("distribution_ordinal")),
                            rdr.GetString(rdr.GetOrdinal("DefaultConstraint")),
                            rdr["collation_name"] == DBNull.Value ? string.Empty : (string)rdr["collation_name"]
                            ));

                    }

                    rdr.Close();
                    if (getStruture)
                    {
                        dbstruct.GetTable(sourceTable).Columns.AddRange(cols);
                        return;
                    }
                }
            }
            else
            {
                cols = dbstruct.GetTable(sourceTable).Columns;
            }

            cols.Sort((a, b) => a.column_id.CompareTo(b.column_id));
        
            foreach (ColumnDef c in cols)
            {

                StringBuilder columnDefinition = new StringBuilder();

                if (c.distrbution_ordinal == 1)
                {
                    // Save name of Distribution column
                    distColumn = c.name;
                }
                if (c.column_id > 1)
                {
                    columnspec.Append("\r\n\t,");
                    columnSelect.Append("\r\n\t,");
                }
                else
                {
                    columnspec.Append("\t");
                    columnSelect.Append("\t");
                }

                columnDefinition.Append("[" + c.name + "]" + "\t" + c.type + "\t");
                columnspec.Append("[" + c.name + "]" + "\t" + c.type + "\t");
                columnSelect.Append(c.name);
                if (c.type == "bigint" ||
                    c.type == "bit" ||
                    c.type == "date" ||
                    c.type == "datetime" ||
                    c.type == "int" ||
                    c.type == "smalldatetime" ||
                    c.type == "smallint" ||
                    c.type == "smallmoney" ||
                    c.type == "money" ||
                    c.type == "tinyint" ||
                    c.type == "real")
                {
                    // no size params
                }

                else if (
                    c.type == "binary" ||
                    c.type == "varbinary")
                {
                    // max_length only
                    columnspec.Append("(");
                    columnspec.Append(c.max_length);
                    columnspec.Append(")\t");


                    columnDefinition.Append("(");
                    columnDefinition.Append(c.max_length);
                    columnDefinition.Append(")\t");

                }

                else if (
                    c.type == "char" ||
                    c.type == "varchar")
                {
                    // max_length only
                    columnspec.Append("(");
                    columnspec.Append(c.max_length);
                    columnspec.Append(")\t");
                    columnspec.Append("COLLATE\t");
                    columnspec.Append(c.collation_name);
                    columnspec.Append("\t");


                    columnDefinition.Append("(");
                    columnDefinition.Append(c.max_length);
                    columnDefinition.Append(")\t");
                    columnDefinition.Append("COLLATE\t");
                    columnDefinition.Append(c.collation_name);
                    columnDefinition.Append("\t");

                }

                else if (
                    c.type == "nchar" ||
                    c.type == "nvarchar")
                {
                    // max_length only
                    columnspec.Append("(");
                    columnspec.Append(c.max_length / 2);
                    columnspec.Append(")\t");
                    columnspec.Append("COLLATE\t");
                    columnspec.Append(c.collation_name);
                    columnspec.Append("\t");

                    columnDefinition.Append("(");
                    columnDefinition.Append(c.max_length / 2);
                    columnDefinition.Append(")\t");
                    columnDefinition.Append("COLLATE\t");
                    columnDefinition.Append(c.collation_name);
                    columnDefinition.Append("\t");

                }

                else if (
                    c.type == "float")
                {
                    // precision only
                    columnspec.Append("(");
                    columnspec.Append(c.precision);
                    columnspec.Append(")\t");


                    columnDefinition.Append("(");
                    columnDefinition.Append(c.precision);
                    columnDefinition.Append(")\t");
                }

                else if (
                    c.type == "datetime2" ||
                    c.type == "datetimeoffset" ||
                    c.type == "time")
                {
                    // Scale only
                    columnspec.Append("(");
                    columnspec.Append(c.scale);
                    columnspec.Append(")\t");

                    columnDefinition.Append("(");
                    columnDefinition.Append(c.scale);
                    columnDefinition.Append(")\t");

                }

                else if (
                    c.type == "decimal" ||
                    c.type == "numeric")
                {
                    // Precision and Scale
                    columnspec.Append("(");
                    columnspec.Append(c.precision);
                    columnspec.Append(",");
                    columnspec.Append(c.scale);
                    columnspec.Append(")\t");

                    columnDefinition.Append("(");
                    columnDefinition.Append(c.precision);
                    columnDefinition.Append(",");
                    columnDefinition.Append(c.scale);
                    columnDefinition.Append(")\t");

                }

                else
                {
                    Exception e = new Exception("Unsupported Type " + c.type + " for column : "+c.name+" - Table : "+ sourceTable);
                    throw e;
                }

                columnspec.Append(c.is_nullable ? "NULL" : "NOT NULL"); 

                columnDefinition.Append(c.is_nullable ? "NULL" : "NOT NULL");

                columnspec.Append(" " + c.defaultconstraint);
                ColumnDef current = cols[cols.IndexOf(c)];
                current.columnDefinition = columnDefinition.ToString();
                tempCols.Add(current);

            }
            columnClause = columnspec.ToString();


            cols = tempCols;

            if (cols.Count == 0)
            {
                // invalid query
                throw new Exception("Unable to retrieve column data for " + sourceTable + " in database " + sourceDb);
            }
        }
        private void getClusteredIndexGlobal()
        {
            clusteredCols.Clear();
            string TableKey = "";
            string SchemaName;
            string TableName;
            string tableKeyPrevious = "";
            TableSt TableStruct = new TableSt();
            string clusterindexname = string.Empty;
            string clusterindexnamePrevious = string.Empty;

            cmd.CommandText =
               "select schema_name(tbl.schema_id) as SchemaName,tbl.Name as TableName,i.key_ordinal, c.name, i.is_descending_key, si.[type] as index_type ,si.name as indexname " +
               "from sys.indexes si " +
               "inner join sys.tables tbl on tbl.object_id=Si.object_id and tbl.type = 'U' " +
               "left join sys.index_columns i on i.object_id = si.object_id " +
               "left join sys.columns c on c.column_id = i.column_id and c.object_id = i.object_id " +
               "where i.index_id = 1 and si.[type] <> 2 " +
               "order by schema_name(tbl.schema_id),tbl.name,key_ordinal ";

            rdr = cmd.ExecuteReader();
            


            while (rdr.Read())
            {
                SchemaName = rdr.GetString(rdr.GetOrdinal("SchemaName"));
                TableName = rdr.GetString(rdr.GetOrdinal("TableName"));
                clusterindexname = rdr.GetString(rdr.GetOrdinal("indexname"));
                TableKey = SchemaName + "." + TableName;
                if (TableKey != tableKeyPrevious)
                {
                    if (clusteredCols.Count != 0 && (TableStruct != null))
                    {
                        TableStruct.clusteredcols.AddRange(clusteredCols);
                        TableStruct.ClusteredIndexName = clusterindexnamePrevious;
                    }
                    TableStruct = this.dbstruct.GetTable(TableKey);
                    clusteredCols.Clear();
                    tableKeyPrevious = TableKey;
                    clusterindexnamePrevious = clusterindexname;
                }

                             
                clusteredCols.Add(new IndexColumnDef(
                    rdr.GetByte(rdr.GetOrdinal("key_ordinal")),
                    rdr.GetString(rdr.GetOrdinal("name")),
                    rdr.GetBoolean(rdr.GetOrdinal("is_descending_key")),
                    rdr.GetByte(rdr.GetOrdinal("index_type"))
                    ));
                }
            if (TableKey != "" && TableStruct != null)
            {
                TableStruct.clusteredcols.AddRange(clusteredCols);
                TableStruct.ClusteredIndexName = clusterindexname;
            }
            rdr.Close();

        }
        private void getClusteredIndex(Boolean GetStructure, Boolean SourceFromFile)
        {
            clusteredCols.Clear();
            clusteredClause = "";
            StringBuilder clusteredspec = new StringBuilder();
            Boolean isCCI = false;

            if (!SourceFromFile)
            {

                cmd.CommandText =
                "select i.key_ordinal, c.name, i.is_descending_key, si.[type] as index_type ,si.name as indexname " +
                "from sys.indexes si " +
                "left join sys.index_columns i on i.object_id = si.object_id " +
                "left join sys.columns c on c.column_id = i.column_id and c.object_id = i.object_id " +
                "where i.index_id = 1 and si.[type] <> 2 and " +
                "i.object_id = (select object_id from sys.tables where schema_name(schema_id) + '.' + name = '" + sourceTable + "') " +
                "and i.partition_ordinal = 0" + 
                "order by key_ordinal ";

                rdr = cmd.ExecuteReader();
                string clusterindexname = string.Empty;
                if (rdr.HasRows)  
                {
                    while (rdr.Read())
                    {
                        if (clusterindexname == string.Empty)
                            clusterindexname = rdr.GetString(rdr.GetOrdinal("indexname"));

                        clusteredCols.Add(new IndexColumnDef(
                            rdr.GetByte(rdr.GetOrdinal("key_ordinal")),
                            rdr.GetString(rdr.GetOrdinal("name")),
                            rdr.GetBoolean(rdr.GetOrdinal("is_descending_key")),
                            rdr.GetByte(rdr.GetOrdinal("index_type"))
                            ));
                    }
                }
                rdr.Close();

                if (GetStructure)
                {
                    dbstruct.GetTable(sourceTable).ClusteredIndexName = clusterindexname;
                    dbstruct.GetTable(sourceTable).clusteredcols.AddRange(clusteredCols);
                    return;
                }
            }
            else
            {
                // get definition from file structure
                clusteredCols = dbstruct.GetTable(sourceTable).clusteredcols;
            }


            foreach (IndexColumnDef i in clusteredCols.OrderBy(o => o.key_ordinal))
            {
                if (i.index_type == 5) // CCI
                {
                    isCCI = true;
                    clusteredspec.Append("CLUSTERED COLUMNSTORE INDEX");
                    break;
                }
                if (i.key_ordinal > 1)
                {
                    clusteredspec.Append("\r\n,");
                }
                else
                {
                    clusteredspec.Append("CLUSTERED INDEX (");
                }
                clusteredspec.Append("[" + i.name + "]" + (i.is_descending_key ? " DESC " : ""));
            }
            if (clusteredspec.Length > 0 && !isCCI)
            {
                clusteredspec.Append(")");
            }

            clusteredClause = clusteredspec.ToString();
        }
        private void getNonclusteredIndexesGlobal()
        {
            nonclusteredIndexes.Clear();
            string TableKey = "";
            string SchemaName;
            string TableName;
            string tableKeyPrevious = "";
            TableSt TableStruct = new TableSt();
            string idxnamePrevious = "";
            string idxname = "";
            NonclusteredIndexDef ncidef = null;

            cmd.CommandText = @"select tbl.Name as TableName,schema_name(schema_id) as SchemaName,ix.name as index_name, i.key_ordinal, c.name, i.is_descending_key
                                    from sys.index_columns i
                                        inner join sys.tables tbl on tbl.object_id = i.object_id and tbl.type = 'U'
                                            join sys.indexes ix on ix.index_id = i.index_id and ix.object_id = i.object_id
                                                join sys.columns c on c.column_id = i.column_id  and c.object_id = ix.object_id
                                                where i.key_ordinal > 0
                                                and i.index_id > 1-- NonClustered Indexes
                                                 order by schema_name(tbl.schema_id),tbl.name,ix.name, key_ordinal";
            rdr = cmd.ExecuteReader();

           while (rdr.Read())
                {

                    SchemaName = rdr.GetString(rdr.GetOrdinal("SchemaName"));
                    TableName = rdr.GetString(rdr.GetOrdinal("TableName"));
                    idxname = rdr.GetString(rdr.GetOrdinal("index_name"));
                    TableKey = SchemaName + "." + TableName;
                    if (TableKey != tableKeyPrevious)
                    {
                        if (nonclusteredIndexes.Count != 0 && TableStruct != null)
                        {
                            TableStruct.nonclusteredIndexes.AddRange(nonclusteredIndexes);
                        }
                        TableStruct = this.dbstruct.GetTable(TableKey);
                        nonclusteredIndexes.Clear();
                        tableKeyPrevious = TableKey;
                    }

                if (TableStruct != null)
                {
                    if (idxname != idxnamePrevious)
                    {
                        ncidef = new NonclusteredIndexDef(idxname);
                        nonclusteredIndexes.Add(ncidef);
                        idxnamePrevious = idxname;
                        TableStruct.nonclusteredIndexes.Add(new NonclusteredIndexDef(idxname));
                    }

                    TableStruct.GetIndex(idxname).cols.Add(new IndexColumnDef(
                          rdr.GetByte(rdr.GetOrdinal("key_ordinal")),
                          rdr.GetString(rdr.GetOrdinal("name")),
                          rdr.GetBoolean(rdr.GetOrdinal("is_descending_key")),
                          0
                          ));
                }
                }
            

            rdr.Close();


        }
        private void getNonclusteredIndexes(Boolean GetStructure, Boolean SourceFromFile)
        {
            nonclusteredIndexes.Clear();
            nonClusteredClause = "";
            StringBuilder nonclusteredspec = new StringBuilder();
            if (!SourceFromFile)
            {
                cmd.CommandText =
                    "select ix.name as index_name, i.key_ordinal, c.name, i.is_descending_key " +
                    "from sys.index_columns i " +
                    "join sys.indexes ix on ix.index_id = i.index_id and ix.object_id = i.object_id " +
                    "join sys.columns c on c.column_id = i.column_id  and c.object_id = ix.object_id " +
                    "where i.key_ordinal > 0 and " +
                    "i.object_id = (select object_id from sys.tables where schema_name(schema_id) + '.' + name = '" + sourceTable + "') and " +
                    "i.index_id > 1 " +   // NonClustered Indexes
                    "order by ix.name, key_ordinal ";

                rdr = cmd.ExecuteReader();

                if (rdr.HasRows)  
                {
                    string idxname = "";
                    NonclusteredIndexDef ncidef = null;
                    while (rdr.Read())
                    {
                        if (idxname != rdr.GetString(rdr.GetOrdinal("index_name")))
                        {
                            idxname = rdr.GetString(rdr.GetOrdinal("index_name"));
                            ncidef = new NonclusteredIndexDef(idxname);
                            nonclusteredIndexes.Add(ncidef);

                            if (GetStructure)
                            {
                                dbstruct.GetTable(sourceTable).nonclusteredIndexes.Add(ncidef);
                            }

                        }

                        if (!GetStructure)
                        {
                            ncidef.cols.Add(new IndexColumnDef(
                                rdr.GetByte(rdr.GetOrdinal("key_ordinal")),
                                rdr.GetString(rdr.GetOrdinal("name")),
                                rdr.GetBoolean(rdr.GetOrdinal("is_descending_key")),
                                0
                                ));
                        }
                        else
                        {
                            dbstruct.GetTable(sourceTable).GetIndex(idxname).cols.Add(new IndexColumnDef(
                                rdr.GetByte(rdr.GetOrdinal("key_ordinal")),
                                rdr.GetString(rdr.GetOrdinal("name")),
                                rdr.GetBoolean(rdr.GetOrdinal("is_descending_key")),
                                0
                                ));
                        }

                    }
                }

                rdr.Close();
                if (GetStructure)
                {
                    return;
                }
            }
            else
            {
                nonclusteredIndexes = dbstruct.GetTable(sourceTable).nonclusteredIndexes;
            }
            foreach (NonclusteredIndexDef ncidef in nonclusteredIndexes)
            {
                nonclusteredspec.Append("CREATE INDEX [" + ncidef.name + "] ON [" + destDb + "].[" + destTable.Replace(".", "].[") + "] ");
                foreach (IndexColumnDef i in ncidef.cols)
                {
                    if (i.key_ordinal > 1)
                    {
                        nonclusteredspec.Append("\r\n,");
                    }
                    else
                    {
                        nonclusteredspec.Append("\r\n(");
                    }
                    nonclusteredspec.Append("[" + i.name + "]" + (i.is_descending_key ? " DESC " : ""));
                }
                nonclusteredspec.Append(");\r\n");
            }
            nonClusteredClause = nonclusteredspec.ToString();
        }

        private void getStatsGlobal()
        {
            stats.Clear();
            string statname = "";
            StatDef statdef = null;
            string TableKey = "";
            string SchemaName;
            string TableName;
            string tableKeyPrevious = "";
            TableSt TableStruct = new TableSt();
            string statnamePrevious = "";


            cmd.CommandText =
               "select schema_name(tbl.schema_id) as SchemaName,tbl.name as TableName,s.name as stat_name, sc.stats_column_id, c.name " +
               "from sys.stats s " +
                "inner join sys.tables tbl on tbl.object_id=s.object_id and tbl.type = 'U' " +
               "join sys.stats_columns sc on s.stats_id = sc.stats_id and s.object_id = sc.object_id " +
               "join sys.columns c on c.column_id = sc.column_id  and c.object_id = sc.object_id " +
              "and user_created=1 " +
               "order by schema_name(tbl.schema_id),tbl.name,s.name, sc.stats_column_id ";

            rdr = cmd.ExecuteReader();

            while (rdr.Read())
            {
                SchemaName = rdr.GetString(rdr.GetOrdinal("SchemaName"));
                TableName = rdr.GetString(rdr.GetOrdinal("TableName"));
                statname = rdr.GetString(rdr.GetOrdinal("stat_name"));
                TableKey = SchemaName + "." + TableName;
                if (TableKey != tableKeyPrevious)
                {
                    if (stats.Count != 0 && TableStruct != null)
                    {
                        TableStruct.statistics.AddRange(stats);
                    }
                    TableStruct = this.dbstruct.GetTable(TableKey);
                    stats.Clear();
                    tableKeyPrevious = TableKey;

                }

                if (TableStruct != null)
                {
                    if (statname != statnamePrevious)
                    {
                        statname = rdr.GetString(rdr.GetOrdinal("stat_name"));
                        statdef = new StatDef(statname);
                        TableStruct.statistics.Add(statdef);
                        statnamePrevious = statname;
                    }


                    statdef.cols.Add(new StatColumnDef(
                        rdr.GetInt32(rdr.GetOrdinal("stats_column_id")),
                        rdr.GetString(rdr.GetOrdinal("name"))
                        ));
                }

                }

           rdr.Close();
        }
        private void getStats(Boolean GetStructure, Boolean SourceFromFile)
        {
            stats.Clear();
            statsClause = "";
            StringBuilder statspec = new StringBuilder();

            if (!SourceFromFile)
            {
                cmd.CommandText =
                "select s.name as stat_name, sc.stats_column_id, c.name, has_filter, filter_definition " +
                "from sys.stats s " +
                "join sys.stats_columns sc on s.stats_id = sc.stats_id and s.object_id = sc.object_id " +
                "join sys.columns c on c.column_id = sc.column_id  and c.object_id = sc.object_id " +
                "where s.object_id = (select object_id from sys.tables where schema_name(schema_id) + '.' + name = '" + sourceTable + "') " +
                "and user_created=1 " +
                "order by s.name, sc.stats_column_id ";

                rdr = cmd.ExecuteReader();

                if (rdr.HasRows)  
                {
                    string statname = "";
                    StatDef statdef = null;
                    while (rdr.Read())
                    {


                            if (statname != rdr.GetString(rdr.GetOrdinal("stat_name")))
                            {
                                statname = rdr.GetString(rdr.GetOrdinal("stat_name"));
                                statdef = new StatDef(statname);
                                if (!(rdr.IsDBNull(rdr.GetOrdinal("filter_definition"))))
                                statdef.filter = rdr.GetString(rdr.GetOrdinal("filter_definition"));
                                stats.Add(statdef);
                                if (!GetStructure)
                                {
                                    //no further steps needed
                                }
                                else
                                {
                                    //adding stat definition to database structure
                                    dbstruct.GetTable(sourceTable).statistics.Add(statdef);
                                }
                                
                                
                            }
                            statdef.cols.Add(new StatColumnDef(
                                rdr.GetInt32(rdr.GetOrdinal("stats_column_id")),
                                rdr.GetString(rdr.GetOrdinal("name"))
                                ));


                    }
                }

                rdr.Close();
                if (GetStructure)
                {
                    return;
                }
            }
            else
            {
                // retrieve stats from dbstruct from json file
                stats.AddRange(dbstruct.GetTable(sourceTable).statistics);
            }
            foreach (StatDef statdef in stats)
            {
                statspec.Append(buildCreateStatisticsText(statdef,destTable));

            }
            statsClause = statspec.ToString();
        }

        private void getPartitioningGlobal ()
        {
            string TableKey = "";
            string SchemaName;
            string TableName;
            string tableKeyPrevious = "";
            TableSt TableStruct = new TableSt();
            partitionColumn = null;
            partitionLeftOrRight = null;
            partitionBoundaryClause = "";
            StringBuilder partitionspec = new StringBuilder();
            partitionBoundaries.Clear();

            cmd.CommandText =@"select schema_name(tbl.schema_id) as SchemaName,tbl.name as TableName,c.name,pf.boundary_value_on_right
                                from sys.tables tbl
                                    join sys.indexes i on (i.object_id = tbl.object_id and i.index_id < 2) 
			                            join sys.index_columns ic on(ic.partition_ordinal > 0 and ic.index_id = i.index_id and ic.object_id = tbl.object_id)
                                           join sys.columns c on(c.object_id = ic.object_id and c.column_id = ic.column_id)
                                                JOIN sys.data_spaces ds on i.data_space_id = ds.data_space_id
                                                    JOIN sys.partition_schemes ps on ps.data_space_id = ds.data_space_id
                                                        JOIN sys.partition_functions pf on pf.function_id = ps.function_id
                                                        order by schema_name(tbl.schema_id),tbl.name";

            rdr = cmd.ExecuteReader();

            while (rdr.Read())
            {
                SchemaName = rdr.GetString(rdr.GetOrdinal("SchemaName"));
                TableName = rdr.GetString(rdr.GetOrdinal("TableName"));
                partitionColumn = rdr.GetString(rdr.GetOrdinal("name"));
                partitionLeftOrRight = ((bool)rdr.GetBoolean(rdr.GetOrdinal("boundary_value_on_right")) ? "RIGHT" : "LEFT");
                TableKey = SchemaName + "." + TableName;
              
                TableStruct = this.dbstruct.GetTable(TableKey);
                TableStruct.DBPartition.partitionColumn = partitionColumn;
                TableStruct.DBPartition.partitionLeftOrRight = partitionLeftOrRight;

            }
            rdr.Close();
            // get partitions boundaries
            TableStruct = new TableSt();
            cmd.CommandText = @"select  schema_name(tbl.schema_id) as SchemaName,tbl.name as TableName,cast(sp.partition_number as int) as partition_number , prv.value as boundary_value, lower(sty.name) as boundary_value_type 
                        from sys.tables st join sys.indexes si on st.object_id = si.object_id and si.index_id <2
                        join sys.partitions sp on sp.object_id = st.object_id 
                        and sp.index_id = si.index_id 
                        join sys.partition_schemes ps on ps.data_space_id = si.data_space_id 
                        join sys.partition_range_values prv on prv.function_id = ps.function_id 
                        join sys.partition_parameters pp on pp.function_id = ps.function_id 
                        join sys.types sty on sty.user_type_id = pp.user_type_id 
                        and prv.boundary_id = sp.partition_number 
						JOIN sys.tables Tbl on si.object_id = Tbl.object_id
                        order by schema_name(tbl.schema_id),tbl.name,sp.partition_number";

             rdr = cmd.ExecuteReader();


             while (rdr.Read())
             {
                SchemaName = rdr.GetString(rdr.GetOrdinal("SchemaName"));
                TableName = rdr.GetString(rdr.GetOrdinal("TableName"));
                TableKey = SchemaName + "." + TableName;

                if (TableKey != tableKeyPrevious)
                {
                    if ( TableStruct.DBPartition.partitionColumn !="")
                    {
                        TableStruct.DBPartition.partitionBoundaries.AddRange(partitionBoundaries);
                    }
                    TableStruct = this.dbstruct.GetTable(TableKey);
                    partitionBoundaries.Clear();
                    tableKeyPrevious = TableKey;

                }

                if (TableStruct != null)
                {
                    partitionBoundaries.Add(new PartitionBoundary(
                                                 rdr.GetInt32(rdr.GetOrdinal("partition_number")),
                                                 rdr.GetValue(rdr.GetOrdinal("boundary_value")).ToString(),
                                                 rdr.GetString(rdr.GetOrdinal("boundary_value_type"))
                                                 ));
                }
         
             }

            if (TableKey != "" && TableStruct != null)
            {
                TableStruct.DBPartition.partitionBoundaries.AddRange(partitionBoundaries);
            }


            rdr.Close();
        }
        private void getPartitioning(Boolean GetStructure, Boolean SourceFromFile)
        {
            partitionColumn = null;
            partitionLeftOrRight = null;
            partitionBoundaryClause = "";
            StringBuilder partitionspec = new StringBuilder();
            partitionBoundaries.Clear();
            if (!SourceFromFile)
            {
                cmd.CommandText =
                    "select c.name " +
                    "from  sys.tables t " +
                    "join  sys.indexes i on(i.object_id = t.object_id and i.index_id < 2) " +
                    "join  sys.index_columns  ic on(ic.partition_ordinal > 0 " +
                    "and ic.index_id = i.index_id and ic.object_id = t.object_id) " +
                    "join  sys.columns c on(c.object_id = ic.object_id " +
                    "and c.column_id = ic.column_id) " +
                    "where t.object_id  = (select object_id from sys.tables where schema_name(schema_id) + '.' + name = '" + sourceTable + "')  ";
                partitionColumn = (string)cmd.ExecuteScalar();

                if (partitionColumn != null)
                {

                    cmd.CommandText =
                        "select pf.boundary_value_on_right from sys.partition_functions pf " +
                        "JOIN sys.partition_schemes ps on pf.function_id=ps.function_id " +
                        "JOIN sys.data_spaces ds on ps.data_space_id = ds.data_space_id " +
                        "JOIN sys.indexes si on si.data_space_id = ds.data_space_id " +
                        "WHERE si.object_id = (select object_id from sys.tables where schema_name(schema_id) + '.' + name = '" + sourceTable + "') ";
                    partitionLeftOrRight = ((bool)cmd.ExecuteScalar() ? "RIGHT" : "LEFT");

                    cmd.CommandText =
                        "select  cast(sp.partition_number as int) as partition_number , prv.value as boundary_value, lower(sty.name) as boundary_value_type " +
                        "from sys.tables st join sys.indexes si on st.object_id = si.object_id and si.index_id <2" +
                        "join sys.partitions sp on sp.object_id = st.object_id " +
                        "and sp.index_id = si.index_id " +
                        "join sys.partition_schemes ps on ps.data_space_id = si.data_space_id " +
                        "join sys.partition_range_values prv on prv.function_id = ps.function_id " +
                        "join sys.partition_parameters pp on pp.function_id = ps.function_id " +
                        "join sys.types sty on sty.user_type_id = pp.user_type_id " +
                        "and prv.boundary_id = sp.partition_number " +
                        "where st.object_id = (select object_id from sys.tables where schema_name(schema_id) + '.' + name = '" + sourceTable + "')  " +
                        "order by sp.partition_number ";

                    rdr = cmd.ExecuteReader();

                    if (rdr.HasRows) 
                    {
                        while (rdr.Read())
                        {
                            partitionBoundaries.Add(new PartitionBoundary(
                                rdr.GetInt32(rdr.GetOrdinal("partition_number")),
                                rdr.GetValue(rdr.GetOrdinal("boundary_value")).ToString(),
                                rdr.GetString(rdr.GetOrdinal("boundary_value_type"))
                                ));
                        }
                    }

                    rdr.Close();
                    if (GetStructure)
                    {
                        dbstruct.GetTable(sourceTable).DBPartition.partitionColumn = partitionColumn;
                        dbstruct.GetTable(sourceTable).DBPartition.partitionLeftOrRight = partitionLeftOrRight;
                        dbstruct.GetTable(sourceTable).DBPartition.partitionBoundaries = partitionBoundaries;

                    }
                }
            }
            else
            {
                partitionBoundaries = dbstruct.GetTable(sourceTable).DBPartition.partitionBoundaries;
                partitionColumn = dbstruct.GetTable(sourceTable).DBPartition.partitionColumn;
                partitionLeftOrRight = dbstruct.GetTable(sourceTable).DBPartition.partitionLeftOrRight;
            }
            foreach (PartitionBoundary b in partitionBoundaries)
            {
                if (b.partition_number > 1)
                {
                    partitionspec.Append("\r\n,");
                }
                partitionspec.Append("N'" + b.boundary_value + "'");
            }
            if (partitionspec.Length > 0)
            {
                partitionspec.Append(")");
            }


            partitionBoundaryClause = partitionspec.ToString();
        }


        private void getDML(PDWscripter c, string outFile)
        {


            StreamWriter sw = null;
            FileStream fs = null;
            Regex r = new Regex(this.ExcludeObjectSuffixList, RegexOptions.IgnoreCase);
            string ModuleName;

            if (outFile != "")
            {
                fs = new FileStream(outFile, FileMode.Create);
                sw = new StreamWriter(fs);
            }

            Console.Write("DML>");


            // get generated statistics
            String description = "-- script generated the : " + String.Format("{0:d/M/yyyy HH:mm:ss}", DateTime.Now);
            cmd.CommandText = "select count(1) from sys.sql_modules";
            int objectcount = (int)cmd.ExecuteScalar();
            description += "\r\n-- objects scripted - objects = " + objectcount.ToString();
            Console.Write("objects scripted - objects = " + objectcount.ToString());
            sw.WriteLine(description);



            // Adaptation to sort by dependency
            cmd.CommandText = "select definition, object_name(object_id) from sys.sql_modules order by definition;";

            rdr = cmd.ExecuteReader();




            String createUseDbTxt = "USE " + c.sourceDb + "\r\nGO\r\n";
            sw.WriteLine(createUseDbTxt);

            List<KeyValuePair<String, String>> lstDbObjectDefinitions = new List<KeyValuePair<string, string>>();

            

            while (rdr.Read())
            {
   
                ModuleName = rdr.GetString(1);
                    if (!r.IsMatch(ModuleName))
                    {
                        IDataRecord record = (IDataRecord)rdr;
                        KeyValuePair<String, String> kvpObjNameDef = new KeyValuePair<String, String>(String.Format("{0}", record[1]), String.Format("{0}", record[0]));


                        if (!lstDbObjectDefinitions.Exists(objDef => objDef.Key == kvpObjNameDef.Key))
                        {

                            // Object doesn't exist
                            if (!lstDbObjectDefinitions.Any(objDef => objDef.Value.Contains(kvpObjNameDef.Key)))
                            {
                                // Object never used by an other object
                                
                                lstDbObjectDefinitions.Add(kvpObjNameDef);
                            }
                            else
                            {
                                // Object already used by an other object, we had it previously to the calling one
                                int idxCallingObj = lstDbObjectDefinitions.IndexOf(lstDbObjectDefinitions.First(objDef => objDef.Value.Contains(kvpObjNameDef.Key)));
                               
                                lstDbObjectDefinitions.Insert(idxCallingObj, kvpObjNameDef);
                            }
                        }
                    }
            }
            int nbObjectDefinitions = lstDbObjectDefinitions.Count();
            int index = 0;
            string prevDbObjectDefinition = String.Empty;
            foreach (KeyValuePair<String, String> dbObjectDefinition in lstDbObjectDefinitions)
            {
                if (prevDbObjectDefinition != dbObjectDefinition.Value)
                {
                    prevDbObjectDefinition = dbObjectDefinition.Value;

                    if (scriptMode == "Delta")
                    {
                        // Add object Drop
                        string strObjectFullName = dbObjectDefinition.Value.Substring(12, dbObjectDefinition.Value.IndexOf(' ', 13) - 12);
                        
                        string strObjectType = dbObjectDefinition.Value.Substring(7, 4);
                        
                        string strDropObjectQuery = "DROP " + strObjectType + " " + strObjectFullName + ";";
                        sw.WriteLine(strDropObjectQuery);
                        sw.WriteLine("");
                        if (index != nbObjectDefinitions - 1)
                        {
                            sw.WriteLine("GO");
                        }
                        
                        sw.WriteLine("");
                        sw.WriteLine("");
                    }

                    // Add object Create
                    sw.WriteLine(dbObjectDefinition.Value.ToString());
                    sw.WriteLine("");

                    sw.WriteLine("GO");

                    sw.WriteLine("");
                    sw.WriteLine("");
                    Console.Write(".");
                }
                index++;
            }
            sw.WriteLine("PRINT 'END'");
            if (outFile != "")
            {
                sw.Close();
            }
            rdr.Close();
            Console.WriteLine("done");

        }


        private string buildAlterTableForDistributionPolicyChangeText (TableSt SourceTbl)
        {
            StringBuilder ScriptAtlterTableDistribution = new StringBuilder();
            string[] substringsTableName = SourceTbl.name.Split('.');
            string sourcetableSchemaOnly = substringsTableName[0];
            string sourcetablenameOnly = substringsTableName[1];
            string copyTmpTableNameOnly = substringsTableName[1] + "_tmp";

            string copyTmpTableName = "[" + substringsTableName[0] + "].[" + copyTmpTableNameOnly + "]";

            copyDataToTmpTableTxt = "CREATE TABLE " + copyTmpTableName + "\r\n" +
                    "WITH ( DISTRIBUTION = " + (SourceTbl.distribution_policy == 2 ? ("HASH ([" + SourceTbl.GetDistributionColumn() + "])") : (SourceTbl.distribution_policy == 3 ? "REPLICATE" : "ROUND_ROBIN")) +
                    ")\r\n" +
                    "AS SELECT * FROM [" + SourceTbl.name.Replace(".", "].[") + "]" +
                    "\r\n;\r\n";
            ScriptAtlterTableDistribution.Append(copyDataToTmpTableTxt);
            

            // Drop Table
            dropDeployTableTxt = "DROP TABLE [" + SourceTbl.name.Replace(".", "].[") + "]" +
                    ";\r\n";
            ScriptAtlterTableDistribution.Append(dropDeployTableTxt);
            

            //Rename table
            string RenameTable = "RENAME OBJECT::" + sourcetableSchemaOnly + "." + copyTmpTableNameOnly + " TO " + sourcetablenameOnly + ";\r\n";

            ScriptAtlterTableDistribution.Append(RenameTable);
            
            return ScriptAtlterTableDistribution.ToString();


        }

        private void buildAlterTableForDistributionPolicyChange(StreamWriter sw, TableSt SourceTbl)
        {
            
            string[] substringsTableName = SourceTbl.name.Split('.');
            string sourcetableSchemaOnly = substringsTableName[0];
            string sourcetablenameOnly = substringsTableName[1];
            string copyTmpTableNameOnly = substringsTableName[1] + "_tmp";

            string copyTmpTableName = "[" + substringsTableName[0] + "].[" + copyTmpTableNameOnly + "]";

            copyDataToTmpTableTxt = "CREATE TABLE " + copyTmpTableName + "\r\n" +
                    "WITH ( DISTRIBUTION = " + (SourceTbl.distribution_policy == 2 ? ("HASH ([" + SourceTbl.GetDistributionColumn() + "])") : (SourceTbl.distribution_policy == 3 ? "REPLICATE" : "ROUND_ROBIN")) +
                    ")\r\n" +
                    "AS SELECT * FROM [" + SourceTbl.name.Replace(".", "].[") + "]" +
                    "\r\n;\r\n";

            sw.WriteLine(copyDataToTmpTableTxt);

            // Drop Table
            dropDeployTableTxt = "DROP TABLE [" + SourceTbl.name.Replace(".", "].[") + "]" +
                    ";\r\n";

           sw.WriteLine(dropDeployTableTxt);

            //Rename table
            string RenameTable = "RENAME OBJECT::" + sourcetableSchemaOnly + "." + copyTmpTableNameOnly + " TO " + sourcetablenameOnly + ";\r\n";

            sw.WriteLine(RenameTable);


        }
        private void buildCreateTableText(StreamWriter sw)
        {
            
            destTableFullName = destTable.Replace(".", "].[");
            
            sourceTmpTableName = "[" + destDb + "].[" + destTableFullName.Replace(destTableFullName.Substring(0, destTableFullName.IndexOf(']')), "DEP") + "]";
            if (scriptMode == "Delta")
            {
                // Copy Data table to Temporary Table
                copyDataToTmpTableTxt = "CREATE TABLE " + sourceTmpTableName + "\r\n" +
                    "WITH ( DISTRIBUTION = " + (distribution_policy == 2 ? ("HASH ([" + distColumn + "])") : (distribution_policy == 3 ? "REPLICATE" : "ROUND_ROBIN")) +
                    ")\r\n" +
                    "AS SELECT * FROM [" + destDb + "].[" + destTable.Replace(".", "].[") + "]" +
                    "\r\n;\r\n";

                sw.WriteLine(copyDataToTmpTableTxt);

                // Drop Table
                dropDeployTableTxt = "DROP TABLE [" + destDb + "].[" + destTable.Replace(".", "].[") + "]" +
                    ";\r\n";
                sw.WriteLine(dropDeployTableTxt);
            }
            
            createTableTxt = "CREATE TABLE [" + destTable.Replace(".", "].[") + "]\r\n(\r\n" +
                columnClause + "\r\n)\r\n" +
                "WITH ( DISTRIBUTION = " + (distribution_policy == 2 ? ("HASH ([" + distColumn + "])") : (distribution_policy == 3 ? "REPLICATE" : "ROUND_ROBIN")) +
                
                (clusteredClause != "" ? "\r\n, " + clusteredClause : "") +
                (partitionBoundaryClause != "" ? ("\r\n, PARTITION ([" + partitionColumn + "] RANGE " + partitionLeftOrRight + " FOR VALUES \r\n(" +
                partitionBoundaryClause + ")") : "") +
                ");\r\n" + nonClusteredClause + statsClause;
            sw.WriteLine(createTableTxt);

            if (scriptMode == "Delta")
            {
                // Copy data from Temporary Table
                
                columnSelect = "SET @COLUMNSNAME = NULL \r\n" +
                    "select @COLUMNSNAME = COALESCE(@COLUMNSNAME,'') + c.name + ', ' " +
                "from sys.columns c " +
                "where c.object_id = (select object_id from sys.tables where schema_name(schema_id) + '.' + name = '" + destTable + "') \r\n";
                sw.WriteLine(columnSelect);

                copyDataFromTmpTableTxt = "INSERT INTO [" + destDb + "].[" + destTable.Replace(".", "].[") + "]\r\n(\r\n" +
                    "@COLUMNSNAME)\r\n " +
                    "FROM (SELECT @COLUMNSNAME FROM " + sourceTmpTableName + " \r\n " +
                    ";\r\n";
                sw.WriteLine(copyDataFromTmpTableTxt);

                // Drop Temporary Table
                dropDeployTmpTableTxt = "DROP TABLE " + sourceTmpTableName + "\r\n" +
                    ";\r\n";
                sw.WriteLine(dropDeployTmpTableTxt);
            }

            Console.Write(".");
        }

        

        private string buildScriptCreateTable()
        {

            createTableTxt = "CREATE TABLE [" + destTable.Replace(".", "].[") + "]\r\n(\r\n" +
            columnClause + "\r\n)\r\n" +
            "WITH ( DISTRIBUTION = " + (distribution_policy == 2 ? ("HASH ([" + distColumn + "])") : (distribution_policy == 3 ? "REPLICATE" : "ROUND_ROBIN")) +
            
            (clusteredClause != "" ? "\r\n, " + clusteredClause : "") +
            (partitionBoundaryClause != "" ? ("\r\n, PARTITION ([" + partitionColumn + "] RANGE " + partitionLeftOrRight + " FOR VALUES \r\n(" +
            partitionBoundaryClause + ")") : "") +
            ");\r\n" + nonClusteredClause + statsClause;

            return createTableTxt;
        }

        private void buildCreateTableText(StreamWriter sw, string dbTarget, bool bWarn)
        {

            String strStartWarningMessage = string.Empty;
            String strEndWarningMessage = string.Empty;

            strStartWarningMessage = "/* WARNING !!!! ======:  Possible Distribution changed .\r\n";
            strEndWarningMessage = "*/\r\n\r\n";


            createTableTxt = "CREATE TABLE [" + dbTarget + "].[" + destTable.Replace(".", "].[") + "]\r\n(\r\n" +
                columnClause + "\r\n)\r\n" +
                "WITH ( DISTRIBUTION = " + (distribution_policy == 2 ? ("HASH ([" + distColumn + "])") : (distribution_policy == 3 ? "REPLICATE" : "ROUND_ROBIN")) +
                
                (clusteredClause != "" ? "\r\n, " + clusteredClause : "") +
                (partitionBoundaryClause != "" ? ("\r\n, PARTITION ([" + partitionColumn + "] RANGE " + partitionLeftOrRight + " FOR VALUES \r\n(" +
                partitionBoundaryClause + ")") : "") +
                ");\r\n\r\n" + nonClusteredClause + statsClause;

            if (bWarn)
            {
                createTableTxt = strStartWarningMessage + createTableTxt + strEndWarningMessage;
                writeWarningtxt(createTableTxt);
            }

            sw.WriteLine(createTableTxt);

            Console.Write(".");
        }

        public string  buildAlterTableScript(PDWscripter cSource, PDWscripter cTarget, TableDef t, Boolean SourceFromFile)
        {

            StringBuilder columnspecAdd = new StringBuilder();
            StringBuilder columnspecAlter = new StringBuilder();
            StringBuilder columnsAlterDefaultCnstr = new StringBuilder();
            StringBuilder columnspecDrop = new StringBuilder();
            StringBuilder columnspecDropStat = new StringBuilder();
            StringBuilder alterScript = new StringBuilder();
            StringBuilder alterclusteredindexscript = new StringBuilder();
            string strStartWarningMessage = string.Empty;
            string strWarningAlterMessage = string.Empty;
            string targetdistColumn = string.Empty;
            string strDopStats = string.Empty;
            string strDopStatsTXT = string.Empty;
            string strDistributionChangedTXT = string.Empty;
      
            string strAlterScript;
            TableSt SourceTbl;
            TableSt TargetTbl;


            // Init
            cSource.sourceTable = t.name;
            cSource.destTable = t.name;

            cTarget.sourceTable = t.name;
            cTarget.destTable = t.name;

            SourceTbl = cSource.dbstruct.GetTable(t.name);
            TargetTbl = cTarget.dbstruct.GetTable(t.name);

            // GetColumns
            cSource.getSourceColumns(false, SourceFromFile);
            cTarget.getSourceColumns(false, SourceFromFile);
            targetdistColumn = cTarget.distColumn;

            // If Distribution Changed, not the same object , so not Alter !!!  ==> TODO 
            if (SourceTbl.distribution_policy != TargetTbl.distribution_policy || cTarget.distColumn != cSource.distColumn)
            {

                if (SourceTbl.distribution_policy != TargetTbl.distribution_policy)
                {
                    string strWarningDistibutionTypeChange = "/*  Distribution type changed for table " + SourceTbl.name + " FROM " + TargetTbl.distribution_policy.ToString() + " to " + SourceTbl.distribution_policy + "  */\r\n";
                    alterScript.Append(strWarningDistibutionTypeChange);
                }
                if (cTarget.distColumn != cSource.distColumn)
                {
                    string strWarningDistibutionTypeChange = "/*  Distribution Column changed for table " + SourceTbl.name + " From " + cTarget.distColumn + " to " + cSource.distColumn + "  */\r\n";
                    alterScript.Append(strWarningDistibutionTypeChange);

                }

                alterScript.Append(cSource.buildAlterTableForDistributionPolicyChangeText(SourceTbl));

            }


            //Compare columns
            var ListColumnsToCreateOrAlter = cSource.cols.Except(cTarget.cols).ToList();
            var ListolumnsToDelete = cTarget.cols.Except(cSource.cols).ToList();


            #region CompareColumnsTo ALTER
            int iCountAdd = 0;
            bool isToAdd = false;
            bool isToAddDefaultContraint = false;
            bool isToAlter = false;


            foreach (ColumnDef c in ListColumnsToCreateOrAlter)
            {
                isToAdd = true;
                strStartWarningMessage = "";

                
                foreach (ColumnDef cTemp in cTarget.cols)
                {
                    if (cTemp.name == c.name)
                    {
                        isToAdd = false;
                        isToAlter = (cTemp.columnDefinition != c.columnDefinition);
                        isToAddDefaultContraint = (cTemp.defaultconstraint == "" && c.defaultconstraint != "");

                        break;
                    }
                }

                
                if (isToAdd)
                {
                    iCountAdd += 1;
                }
                else
                {
                    if (isToAlter)
                    {
                        if (cTarget.stats.Exists(x => x.name == c.name))
                        {
                            // Stats not clusturedIndex ==> DROP 
                            strDopStats = "DROP STATISTICS  [" + destTable.Replace(".", "].[") + "].[" + c.name + "];\r\n";
                            columnspecDropStat.Append(strDopStats);
                        }
                    }
                    else
                    {
                        if (isToAddDefaultContraint)
                        {
                            alterTableTxt = "ALTER TABLE [" + cTarget.destDb + "].[" + destTable.Replace(".", "].[") + "]\r\n" +
                             "ADD " + c.defaultconstraint + " FOR [" + c.name + "];\r\n";

                            columnsAlterDefaultCnstr.Append(alterTableTxt);
                            columnsAlterDefaultCnstr.Append("\r\n");
                        }
                        continue;
                    }
                }
                if (c.distrbution_ordinal == 1)
                {
                    // Changing a distribution column is not supported in Parallel Data Warehouse.
                    distColumn = c.name;
      
                }

                if (isToAdd)
                {
                    if (iCountAdd > 1)
                    {
                        columnspecAdd.Append("\r\n\t," + c.columnDefinition);
                    }
                    else
                    {
                        columnspecAdd.Append("\t" + c.columnDefinition);
                    }
                    columnspecAdd.Append(strStartWarningMessage);
                }
                else
                {
                    alterTableTxt = "ALTER TABLE [" + cTarget.destDb + "].[" + destTable.Replace(".", "].[") + "]\r\n" +
                    "ALTER COLUMN " + c.columnDefinition + ";\r\n";

                    columnspecAlter.Append(alterTableTxt);
                    columnspecAlter.Append(strStartWarningMessage);
                    columnspecAlter.Append("\r\n");
                }




            }
            // ====>>> SCRIPTS
            // script : ADD 
            if (columnspecAdd.Length > 0)
            {
                strAlterScript = "ALTER TABLE [" + cTarget.destDb + "].[" + destTable.Replace(".", "].[") + "]\r\n" +
                    "ADD " + columnspecAdd.ToString() + ";\r\n";

                alterScript.Append(strAlterScript);
                // sw.WriteLine(alterTableTxt);
            }

            // script : DROP STATISTICS 
            // Drop Statistics before Alter Table
            if (columnspecDropStat.Length > 0)
            {
                strDopStatsTXT = "-- === DROP STATISTICS before ALTER tables\r\n";
                alterScript.Append(strDopStatsTXT);

                alterScript.Append(columnspecDropStat.ToString());
            }

            // script : ALTER TABLE / ALTER COLUMNS
            if (columnspecAlter.Length > 0)
            {
                alterScript.Append(columnspecAlter.ToString());

            }
            #endregion

            #region DROP COLUMNS

            int iCountDrop = 0;


            foreach (ColumnDef c in ListolumnsToDelete)
            {
                strStartWarningMessage = string.Empty;
                // If Drop or Modify
                if (cSource.cols.Exists(x => x.name == c.name))
                {
                    continue;
                }
                else
                {

                    iCountDrop += 1;
                }


                if (c.distrbution_ordinal == 1)
                {
                    // Cannot drop distribute Column
                    distColumn = c.name;

                 }

                if (iCountDrop > 1)
                {
                    columnspecDrop.Append("\r\n\t," + "[" + c.name + "]");
                }
                else
                {
                    columnspecDrop.Append("\t" + "[" + c.name + "]");
                }
                columnspecDrop.Append(strStartWarningMessage);

            }


            if (columnspecDrop.Length > 0)
            {

                dropTableTxt = " ALTER TABLE [" + cTarget.destDb + "].[" + destTable.Replace(".", "].[") + "]\r\n" +
                    "DROP COLUMN " + columnspecDrop.ToString() + ";  \r\n";

                alterScript.Append(dropTableTxt);
                string description = "/* WARNING !!!! ======:  DROP COLUMN : " + columnspecDrop.ToString() + " ON TABLE : " + destTable + " \r\n";
                description += dropTableTxt;
               
            }

            #endregion

           

            //Compare clustered index
            if (SourceTbl.clusteredcols.CompareTo(TargetTbl.clusteredcols) == 1)
            {
                alterScript.Append(buildAlterTableClusteredIndexText(SourceTbl, TargetTbl));
            }

            //Compare NonClustered index
            if (SourceTbl.nonclusteredIndexes.CompareTo(TargetTbl.nonclusteredIndexes) == 1)
            {
                alterScript.Append(buildAlterTableNonClusteredIndexText(SourceTbl, TargetTbl));
            }
            //Compare Statistics
            if (SourceTbl.statistics.CompareTo(TargetTbl.statistics) == 1)
            {
                alterScript.Append(buildAlterTableStatisticsText(SourceTbl, TargetTbl));
            }

            // add default constraints
            alterScript.Append(columnsAlterDefaultCnstr);

            return alterScript.ToString();
        }


        private void buildAlterTableText(StreamWriter sw, PDWscripter cSource, PDWscripter cTarget, TableDef t, Boolean SourceFromFile)
        {
            StringBuilder columnspecAdd = new StringBuilder();
            StringBuilder columnspecAlter = new StringBuilder();
            StringBuilder columnsAlterDefaultCnstr = new StringBuilder();
            StringBuilder columnspecDrop = new StringBuilder();
            StringBuilder columnspecDropStat = new StringBuilder();
            StringBuilder alterScript = new StringBuilder();
            StringBuilder alterclusteredindexscript = new StringBuilder();
            string strStartWarningMessage = string.Empty;
            string strWarningAlterMessage = string.Empty;
            string targetdistColumn = string.Empty;
            string strDopStats = string.Empty;
            string strDopStatsTXT = string.Empty;
            string strDistributionChangedTXT = string.Empty;
            bool bIsWarning;
            string strAlterScript;
            TableSt SourceTbl;
            TableSt TargetTbl;


            // Init
            cSource.sourceTable = t.name;
            cSource.destTable = t.name;

            cTarget.sourceTable = t.name;
            cTarget.destTable = t.name;

            SourceTbl = cSource.dbstruct.GetTable(t.name);
            TargetTbl = cTarget.dbstruct.GetTable(t.name);

            // GetColumns
            cSource.getSourceColumns(false, SourceFromFile);
            cTarget.getSourceColumns(false, SourceFromFile);
            targetdistColumn = cTarget.distColumn;

            if (SourceTbl.distribution_policy != TargetTbl.distribution_policy || cTarget.distColumn != cSource.distColumn)
            {

                cSource.buildAlterTableForDistributionPolicyChange(sw, SourceTbl);
                string strWarningDistibutionTypeChange = "/*  Distribution type or column change changed for table " + SourceTbl.name + "\r\n*/\r\n\r\n";
                writeWarningtxt(strWarningDistibutionTypeChange);

            }



      



            //Compare columns
            var ListColumnsToCreateOrAlter = cSource.cols.Except(cTarget.cols).ToList();
            var ListolumnsToDelete = cTarget.cols.Except(cSource.cols).ToList();



            #region CompareColumnsTo ALTER
            int iCountAdd = 0;
            bool isToAdd = false;
            bool isToAddDefaultContraint = false;
            bool isToAlter = false;

            // Init
            bIsWarning = false;


            foreach (ColumnDef c in ListColumnsToCreateOrAlter)
            {
                isToAdd = true;
                strStartWarningMessage = "";

                // If Create Or MODIFY ==> AMD  à améliorer !!!!
                foreach (ColumnDef cTemp in cTarget.cols)
                {
                    if (cTemp.name == c.name)
                    {
                        isToAdd = false;
                        isToAlter = (cTemp.columnDefinition != c.columnDefinition);
                        isToAddDefaultContraint = (cTemp.defaultconstraint == "" && c.defaultconstraint != "");

                        break;
                    }
                }

                //if (cTarget.cols.Exists(x => x.name == c.name))
                if (isToAdd)
                {
                    iCountAdd += 1;
                }
                else
                {
                    if (isToAlter)
                    {
                        if (cTarget.stats.Exists(x => x.name == c.name))
                        {
                            // Stats not clusturedIndex ==> DROP 
                            strDopStats = "DROP STATISTICS  [" + destTable.Replace(".", "].[") + "].[" + c.name + "];\r\n";
                            columnspecDropStat.Append(strDopStats);
                        }
                    }
                    else
                    {
                        if (isToAddDefaultContraint)
                        {
                            alterTableTxt = "ALTER TABLE [" + cTarget.destDb + "].[" + destTable.Replace(".", "].[") + "]\r\n" +
                             "ADD " + c.defaultconstraint + " FOR [" + c.name + "];\r\n";

                            columnsAlterDefaultCnstr.Append(alterTableTxt);
                            columnsAlterDefaultCnstr.Append("\r\n");
                        }
                        continue;
                    }
                }
                if (c.distrbution_ordinal == 1)
                {
                    // Changing a distribution column is not supported in Parallel Data Warehouse.
                    distColumn = c.name;
                    strStartWarningMessage = " -- WARNING !!!! ======: Changing a distribution column is not supported in Parallel Data Warehouse.\r\n";
                    bIsWarning = true;
                    writeWarningtxt(strStartWarningMessage + alterTableTxt);

                }

                if (isToAdd)
                {
                    if (iCountAdd > 1)
                    {
                        columnspecAdd.Append("\r\n\t," + c.columnDefinition);
                    }
                    else
                    {
                        columnspecAdd.Append("\t" + c.columnDefinition);
                    }
                    columnspecAdd.Append(strStartWarningMessage);
                }
                else
                {
                    alterTableTxt = "ALTER TABLE [" + cTarget.destDb + "].[" + destTable.Replace(".", "].[") + "]\r\n" +
                    "ALTER COLUMN " + c.columnDefinition + ";\r\n";

                    columnspecAlter.Append(alterTableTxt);
                    columnspecAlter.Append(strStartWarningMessage);
                    columnspecAlter.Append("\r\n");
                }




            }
            // ====>>> SCRIPTS
            // script : ADD 
            if (columnspecAdd.Length > 0)
            {
                strAlterScript = "ALTER TABLE [" + cTarget.destDb + "].[" + destTable.Replace(".", "].[") + "]\r\n" +
                    "ADD " + columnspecAdd.ToString() + ";\r\n";

                alterScript.Append(strAlterScript);
                // sw.WriteLine(alterTableTxt);
            }

            // script : DROP STATISTICS 
            // Drop Statistics before Alter Table
            if (columnspecDropStat.Length > 0)
            {
                strDopStatsTXT = "-- === DROP STATISTICS before ALTER tables\r\n";
                alterScript.Append(strDopStatsTXT);

                alterScript.Append(columnspecDropStat.ToString());
            }

            // script : ALTER TABLE / ALTER COLUMNS
            if (columnspecAlter.Length > 0)
            {
                alterScript.Append(columnspecAlter.ToString());

            }
            #endregion

            #region DROP COLUMNS

            int iCountDrop = 0;


            foreach (ColumnDef c in ListolumnsToDelete)
            {
                strStartWarningMessage = string.Empty;
                // If Drop or Modify
                if (cSource.cols.Exists(x => x.name == c.name))
                {
                    continue;
                }
                else
                {

                    iCountDrop += 1;
                }


                if (c.distrbution_ordinal == 1)
                {
                    // Cannot drop distribute Column
                    distColumn = c.name;

                    strStartWarningMessage = "-- WARNING !!!! ======:  Cannot drop distribute Column.\r\n";
                    bIsWarning = true;


                }

                if (iCountDrop > 1)
                {
                    columnspecDrop.Append("\r\n\t," + "[" + c.name + "]");
                }
                else
                {
                    columnspecDrop.Append("\t" + "[" + c.name + "]");
                }
                columnspecDrop.Append(strStartWarningMessage);

            }


            if (columnspecDrop.Length > 0)
            {

                dropTableTxt = "/* ALTER TABLE [" + cTarget.destDb + "].[" + destTable.Replace(".", "].[") + "]\r\n" +
                    "DROP COLUMN " + columnspecDrop.ToString() + "; */ \r\n";

                alterScript.Append(dropTableTxt);
                string description = "/* WARNING !!!! ======:  DROP COLUMN : " + columnspecDrop.ToString() + " ON TABLE : " + destTable + " \r\n";
                description += dropTableTxt;
                writeWarningtxt(description);
            }

            #endregion

            //Compare clustered index
            if (SourceTbl.clusteredcols.CompareTo(TargetTbl.clusteredcols) == 1)
            {
                alterScript.Append(buildAlterTableClusteredIndexText(SourceTbl, TargetTbl));
            }

            //Compare NonClustered index
            if (SourceTbl.nonclusteredIndexes.CompareTo(TargetTbl.nonclusteredIndexes) == 1)
            {
                alterScript.Append(buildAlterTableNonClusteredIndexText(SourceTbl, TargetTbl));
            }
            //Compare Statistics
            if (SourceTbl.statistics.CompareTo(TargetTbl.statistics) == 1)
            {
                alterScript.Append(buildAlterTableStatisticsText(SourceTbl, TargetTbl));
            }

            // add default constraints
            alterScript.Append(columnsAlterDefaultCnstr);

            if (alterScript.Length > 0)
                sw.WriteLine(alterScript);

            //Console.Write(".");
            Console.Write(".");
        }
        
        private string buildCreateStatisticsText(StatDef stat, string TargetTbl)
        {
            StringBuilder CreateStatsText = new StringBuilder();
            CreateStatsText.Append("CREATE STATISTICS [" + stat.name + "] ON [" + TargetTbl.Replace(".", "].[") + "] ");
            foreach (StatColumnDef i in stat.cols)
                {
                    if (i.key_ordinal > 1)
                    {
                        CreateStatsText.Append("\r\n,");
                    }
                    else
                    {
                        CreateStatsText.Append("\r\n(");
                    }
                    CreateStatsText.Append("[" + i.name + "] ");
                }
                //managing filters
                CreateStatsText.Append(")");
                if (stat.IsFilteredStat())
                {
                    CreateStatsText.Append("\r\nWHERE "+stat.filter);
                }
                CreateStatsText.Append("\r\n;\r\n");

                return CreateStatsText.ToString();
        }

        private string buildAlterTableStatisticsText(TableSt SourceTbl, TableSt TargetTbl)
        {
            StringBuilder alterstatisticsscript = new StringBuilder();
            foreach (StatDef stat in SourceTbl.statistics)
            {
                StatDef targetstat = TargetTbl.statistics.GetStat(stat.name);
                if (targetstat != null)
                {
                    // index name exists on target table
                    if (stat.CompareTo(targetstat) == 1)
                    {
                        alterstatisticsscript.Append("DROP STATISTICS [" + stat.name + "] ON [" + TargetTbl.name.Replace(".", "].[") + "];\r\n");
                        alterstatisticsscript.Append(buildCreateStatisticsText(stat,TargetTbl.name));
                        
                    }
                }
                else
                {
                    alterstatisticsscript.Append(buildCreateStatisticsText(stat,TargetTbl.name));
                }

            }

            var ListStatisticsToDelete = TargetTbl.statistics.Except(SourceTbl.statistics).ToList();

            foreach (var stattodalete in ListStatisticsToDelete)
            {
                alterstatisticsscript.Append("DROP STATISTICS [" + TargetTbl.name.Replace(".", "].[") + "].[" + stattodalete.name + "];\r\n");
            }

            return alterstatisticsscript.ToString();

        }
        private string buildAlterTableClusteredIndexText(TableSt SourceTbl, TableSt TargetTbl)
        {
            StringBuilder alterclusteredindexscript = new StringBuilder();
            // alterclusteredindexscript.Append("\r\n");

            if (TargetTbl.clusteredcols.Count != 0)
            {
                alterclusteredindexscript.Append("DROP INDEX [" + TargetTbl.ClusteredIndexName + "] ON " + TargetTbl.name + ";\r\n");

            }
            // create clustered index script create
            Boolean isCCI = false;
            foreach (IndexColumnDef i in SourceTbl.clusteredcols.OrderBy(o => o.key_ordinal))
            {
                if (i.index_type == 5) // CCI
                {
                    isCCI = true;
                    foreach (NonclusteredIndexDef ncidx in TargetTbl.nonclusteredIndexes)
                    {
                        alterclusteredindexscript.Append("DROP INDEX [" + ncidx.name + "] ON [" + TargetTbl.name.Replace(".", "].[") + "];\r\n");
                    }
                    TargetTbl.nonclusteredIndexes.Clear();
                    alterclusteredindexscript.Append("CREATE CLUSTERED COLUMNSTORE INDEX [" + SourceTbl.ClusteredIndexName + "] ON " + SourceTbl.name + ";\r\n");
                    break;
                }
                if (i.key_ordinal > 1)
                {
                    alterclusteredindexscript.Append("\r\n,");
                }
                else
                {
                    alterclusteredindexscript.Append("CREATE CLUSTERED INDEX [" + SourceTbl.ClusteredIndexName + "] ON " + SourceTbl.name + "(");
                }
                alterclusteredindexscript.Append("[" + i.name + "]" + (i.is_descending_key ? " DESC " : ""));
            }
            if (alterclusteredindexscript.Length > 0 && !isCCI)
            {
                alterclusteredindexscript.Append(");\r\n");
            }
            return alterclusteredindexscript.ToString();
        }
        private string buildAlterTableNonClusteredIndexText(TableSt SourceTbl, TableSt TargetTbl)
        {
            StringBuilder alternonclusteredindex = new StringBuilder();
            foreach (NonclusteredIndexDef ncidx in SourceTbl.nonclusteredIndexes)
            {
                NonclusteredIndexDef targetncidx = TargetTbl.nonclusteredIndexes.GetIndex(ncidx.name);
                if (targetncidx != null)
                {
                    // index name exists on target table
                    if (ncidx.CompareTo(targetncidx) == 1)
                    {
                        alternonclusteredindex.Append("DROP INDEX [" + ncidx.name + "] ON [" + TargetTbl.name.Replace(".", "].[") + "];\r\n");
                        alternonclusteredindex.Append("CREATE INDEX [" + ncidx.name + "] ON [" + TargetTbl.name.Replace(".", "].[") + "] ");
                        foreach (IndexColumnDef i in ncidx.cols)
                        {
                            if (i.key_ordinal > 1)
                            {
                                alternonclusteredindex.Append("\r\n,");
                            }
                            else
                            {
                                alternonclusteredindex.Append("\r\n(");
                            }
                            alternonclusteredindex.Append("[" + i.name + "]" + (i.is_descending_key ? " DESC " : ""));
                        }
                        alternonclusteredindex.Append(");\r\n");
                    }
                }
                else
                {
                    alternonclusteredindex.Append("CREATE INDEX [" + ncidx.name + "] ON [" + TargetTbl.name.Replace(".", "].[") + "] ");
                    foreach (IndexColumnDef i in ncidx.cols)
                    {
                        if (i.key_ordinal > 1)
                        {
                            alternonclusteredindex.Append("\r\n,");
                        }
                        else
                        {
                            alternonclusteredindex.Append("\r\n(");
                        }
                        alternonclusteredindex.Append("[" + i.name + "]" + (i.is_descending_key ? " DESC " : ""));
                    }
                    alternonclusteredindex.Append(");\r\n");
                }

            }

            var ListNonClusteredIndexToDelete = TargetTbl.nonclusteredIndexes.Except(SourceTbl.nonclusteredIndexes).ToList();

            foreach (var idxtodalete in ListNonClusteredIndexToDelete)
            {
                alternonclusteredindex.Append("DROP INDEX [" + TargetTbl.name.Replace(".", "].[") + "].[" + idxtodalete.name + "];\r\n");
            }

            return alternonclusteredindex.ToString();

        }
        private void compBuildDropTableText(StreamWriter sw, bool bWarn)
        {

            String strStartWarningMessage = string.Empty;
            String strEndWarningMessage = string.Empty;

            strStartWarningMessage = "/* WARNING !!!! ======:  Potential data loss .\r\n";
            strEndWarningMessage = "*/\r\n\r\n";

            destTableFullName = destTable.Replace(".", "].[");


            dropTableTxt = "/* DROP TABLE [" + destDb + "].[" + destTable.Replace(".", "].[") + "] */\r\n";

            if (bWarn)
            {
                dropTableTxt = strStartWarningMessage + dropTableTxt + strEndWarningMessage;
                writeWarningtxt(dropTableTxt);
            }

            sw.WriteLine(dropTableTxt);
            // writeWarningtxt(dropTableTxt);


            Console.Write(".");
        }

      private void compareDML(PDWscripter cSource, PDWscripter cTarget, string outFile, Boolean SourceFromFile, FilterSettings FilterSet)
        {
            StreamWriter outfile = null;
            FileStream fs = null;
            Console.Write("CompareDML>");

            cTarget.cmd.CommandText = @" SELECT c.definition, b.name + '.' + a.name AS ObjectName
                    FROM
                    sys.sql_modules c
                    INNER JOIN sys.objects a ON a.object_id = c.object_id
                    INNER JOIN sys.schemas b
                    ON a.schema_id = b.schema_id";

            if (outFile != "")
            {
                fs = new FileStream(outFile, FileMode.Create);
                outfile = new StreamWriter(fs);
            }
            String description = "-- script generated the : " + String.Format("{0:d/M/yyyy HH:mm:ss}", DateTime.Now) + "\r\n";
            outfile.WriteLine(description);

            String createUseDbTxt = "USE " + cTarget.sourceDb + "\r\nGO\r\n";
            outfile.WriteLine(createUseDbTxt);

            List<KeyValuePair<String, String>> lstSourceDbObjectDefinitions = new List<KeyValuePair<string, string>>();
            List<KeyValuePair<String, String>> lstTargetDbObjectDefinitions = new List<KeyValuePair<string, string>>();

            List<KeyValuePair<String, String>> lstCreateOrAlterDbObjectDefinitions;
            List<KeyValuePair<String, String>> lstDropDbObjectDefinitions;

            if (!SourceFromFile)
            {
                // ===>>>< source
                cSource.cmd.CommandText = @" SELECT c.definition, b.name + '.' + a.name AS ObjectName
                    FROM
                    sys.sql_modules c
                    INNER JOIN sys.objects a ON a.object_id = c.object_id
                    INNER JOIN sys.schemas b
                    ON a.schema_id = b.schema_id";
                rdr = cSource.cmd.ExecuteReader();

                while (rdr.Read())
                {
                    IDataRecord record = (IDataRecord)rdr;
                    KeyValuePair<String, String> sourceKvpObjNameDef = new KeyValuePair<String, String>(String.Format("{0}", record[1]), String.Format("{0}", record[0]).TrimEnd(new char[] { '\r', '\n', ' ' }));

                    if (!lstSourceDbObjectDefinitions.Exists(objDef => objDef.Key == sourceKvpObjNameDef.Key))
                    {

                        // Object doesn't exist
                        if (!lstSourceDbObjectDefinitions.Any(objDef => objDef.Value.Contains(sourceKvpObjNameDef.Key)))
                        {
                            // Object never used by an other object
                            lstSourceDbObjectDefinitions.Add(sourceKvpObjNameDef);
                        }
                        else
                        {
                            // Object already used by an other object, we had it previously to the calling one
                            int idxCallingObj = lstSourceDbObjectDefinitions.IndexOf(lstSourceDbObjectDefinitions.First(objDef => objDef.Value.Contains(sourceKvpObjNameDef.Key)));
                            lstSourceDbObjectDefinitions.Insert(idxCallingObj, sourceKvpObjNameDef);
                        }
                    }
                }
            }
            else
                lstSourceDbObjectDefinitions = DbObjectDefinitions;

            // ===>>>< Target
            rdr = cTarget.cmd.ExecuteReader();

            while (rdr.Read())
            {
                IDataRecord record = (IDataRecord)rdr;
                KeyValuePair<String, String> targetKvpObjNameDef = new KeyValuePair<String, String>(String.Format("{0}", record[1]), String.Format("{0}", record[0]).TrimEnd(new char[] { '\r', '\n', ' ' }));

                if (!lstTargetDbObjectDefinitions.Exists(objDef => objDef.Key == targetKvpObjNameDef.Key))
                {

                    // Object doesn't exist
                    if (!lstTargetDbObjectDefinitions.Any(objDef => objDef.Value.Contains(targetKvpObjNameDef.Key)))
                    {
                        // Object never used by an other object
                        lstTargetDbObjectDefinitions.Add(targetKvpObjNameDef);
                    }
                    else
                    {
                        // Object already used by an other object, we had it previously to the calling one
                        int idxCallingObj = lstTargetDbObjectDefinitions.IndexOf(lstTargetDbObjectDefinitions.First(objDef => objDef.Value.Contains(targetKvpObjNameDef.Key)));
                        lstTargetDbObjectDefinitions.Insert(idxCallingObj, targetKvpObjNameDef);
                    }
                }
            }

            // ==>> Compare
            int toDelete = 0;
            lstCreateOrAlterDbObjectDefinitions = new List<KeyValuePair<string, string>>();
            lstDropDbObjectDefinitions = new List<KeyValuePair<string, string>>();
            List<KeyValuePair<string, string>> lstSourceDbObjectDefinitionsSource;
            List<KeyValuePair<string, string>> lstSourceDbObjectDefinitionsTarget;

            // apply the filter
            switch (FilterSet.Granularity.ToUpper())
            {
                case "SCHEMA":
                    // split to retrieve the schema name moduledef.Key.Split((char)'.')[0] -- schema.name
                    lstSourceDbObjectDefinitionsSource = lstSourceDbObjectDefinitions.FindAll(delegate (KeyValuePair<string, string> moduledef) { return FilterSet.GetSchemas().Contains(moduledef.Key.Split((char)'.')[0]); });
                    lstSourceDbObjectDefinitionsTarget = lstTargetDbObjectDefinitions.FindAll(delegate (KeyValuePair<string, string> moduledef) { return FilterSet.GetSchemas().Contains(moduledef.Key.Split((char)'.')[0]); });

                    lstCreateOrAlterDbObjectDefinitions = lstSourceDbObjectDefinitionsSource.Except(lstSourceDbObjectDefinitionsTarget).ToList();
                    lstDropDbObjectDefinitions = lstSourceDbObjectDefinitionsTarget.Except(lstSourceDbObjectDefinitionsSource).ToList();
                    break;
                case "OBJECTS":
                    lstSourceDbObjectDefinitionsSource = lstSourceDbObjectDefinitions.FindAll(delegate (KeyValuePair<string, string> moduledef) { return FilterSet.GetSchemaNameObjects().Contains(moduledef.Key); });
                    lstSourceDbObjectDefinitionsTarget = lstTargetDbObjectDefinitions.FindAll(delegate (KeyValuePair<string, string> moduledef) { return FilterSet.GetSchemaNameObjects().Contains(moduledef.Key); });

                    lstCreateOrAlterDbObjectDefinitions = lstSourceDbObjectDefinitionsSource.Except(lstSourceDbObjectDefinitionsTarget).ToList();
                    
                    break;


                default:
                    lstCreateOrAlterDbObjectDefinitions = lstSourceDbObjectDefinitions.Except(lstTargetDbObjectDefinitions).ToList();
                    lstDropDbObjectDefinitions = lstTargetDbObjectDefinitions.Except(lstSourceDbObjectDefinitions).ToList();
                    toDelete = lstCreateOrAlterDbObjectDefinitions.Except(lstDropDbObjectDefinitions).ToList().Count();
                    break;
            }


            description = "\r\n-- objects scripted - ALTERorCREATE = " + lstCreateOrAlterDbObjectDefinitions.Count().ToString() + " - DROP = " + toDelete.ToString();
            Console.Write("\r\n objects scripted - ALTERorCREATE = " + lstCreateOrAlterDbObjectDefinitions.Count().ToString() + " - DROP = " + toDelete.ToString());
            outfile.WriteLine(description);
            //==> EnumReferencedObjects ToolBar create OrderedParallelQuery ALTER
            int nbObjectDefinitions = lstCreateOrAlterDbObjectDefinitions.Count();
            int index = 0;
            string prevDbObjectDefinition = String.Empty;

            if (nbObjectDefinitions > 0)
            {
                outfile.WriteLine("/*  =========== DROP AND/OR CREATE ###==> Begin =================*/\r\n");
            }

            // we order the create script based on dependencies
            List<KeyValuePair<string, string>> lstCreateOrAlterDbObjectDefinitionsSanitizedOrdered = new List<KeyValuePair<string, string>>();

            List<KeyValuePair<string, string>> lstCreateOrAlterDbObjectDefinitionsSanitized = new List<KeyValuePair<string, string>>();
            foreach (KeyValuePair<String, String> CreateOrAlterDbObjectDefinition in lstCreateOrAlterDbObjectDefinitions)
            {
                KeyValuePair<String, String> CreateOrAlterDbObjectDefinitionSanitized = new KeyValuePair<String, String>(CreateOrAlterDbObjectDefinition.Key, CreateOrAlterDbObjectDefinition.Value.Replace("[", "").Replace("]", ""));

                lstCreateOrAlterDbObjectDefinitionsSanitized.Add(CreateOrAlterDbObjectDefinitionSanitized);
            }


            foreach (KeyValuePair<String, String> CreateOrAlterDbObjectDefinition in lstCreateOrAlterDbObjectDefinitionsSanitized)
            {
                if (!lstCreateOrAlterDbObjectDefinitionsSanitizedOrdered.Exists(objDef => objDef.Key == CreateOrAlterDbObjectDefinition.Key))
                {

                    // Object doesn't exist
                    if (!lstCreateOrAlterDbObjectDefinitionsSanitizedOrdered.Any(objDef => objDef.Value.Contains(CreateOrAlterDbObjectDefinition.Key)))
                    {
                        // Object never used by an other object
                        lstCreateOrAlterDbObjectDefinitionsSanitizedOrdered.Add(CreateOrAlterDbObjectDefinition);
                    }
                    else
                    {
                        // Object already used by an other object, we had it previously to the calling one
                        int idxCallingObj = lstCreateOrAlterDbObjectDefinitionsSanitizedOrdered.IndexOf(lstCreateOrAlterDbObjectDefinitionsSanitizedOrdered.First(objDef => objDef.Value.Contains(CreateOrAlterDbObjectDefinition.Key)));
                        lstCreateOrAlterDbObjectDefinitionsSanitizedOrdered.Insert(idxCallingObj, CreateOrAlterDbObjectDefinition);
                    }
                }
            }



            foreach (KeyValuePair<String, String> CreateOrAlterDbObjectDefinitionsSanitized in lstCreateOrAlterDbObjectDefinitionsSanitizedOrdered)

            {

                // we retrieve the original value
                KeyValuePair<String, String> dbObjectDefinition = lstCreateOrAlterDbObjectDefinitions.Find(objdef => objdef.Key == CreateOrAlterDbObjectDefinitionsSanitized.Key);



                if (lstTargetDbObjectDefinitions.Exists(objDef => objDef.Key == dbObjectDefinition.Key))
                {
                    index++;

                    // Add object Drop
                    string strObjectFullName = dbObjectDefinition.Value.Substring(12, dbObjectDefinition.Value.IndexOf(' ', 13) - 12);
                    string strObjectType = dbObjectDefinition.Value.Substring(7, 4);
                    string strDropObjectQuery = "DROP " + strObjectType + " " + strObjectFullName + ";";
                    if (index != 1)
                    {
                        strDropObjectQuery = "GO\r\n" + strDropObjectQuery;
                    }
                    outfile.WriteLine(strDropObjectQuery);
                }

                index++;
                // Add object Create

                if (index != 1)
                {
                    outfile.WriteLine("GO");
                }

                outfile.WriteLine(dbObjectDefinition.Value.ToString());
                outfile.WriteLine("GO");

                Console.Write(".");
            }

            if (nbObjectDefinitions > 0)
            {
                outfile.WriteLine("/*  =========== DROP AND/OR CREATE ###==> End ===================*/");
                outfile.WriteLine("PRINT 'DROP AND/OR CREATE End'\r\n");
            }

            // Drop 
            nbObjectDefinitions = lstDropDbObjectDefinitions.Count();

            if (nbObjectDefinitions > 0)
            {
                outfile.WriteLine("/*  ===========        DROP     ###==> Begin =================*/");
                outfile.WriteLine("PRINT 'DROP Begin'\r\n");
            }

            prevDbObjectDefinition = String.Empty;
            foreach (KeyValuePair<String, String> dbObjectDefinition in lstDropDbObjectDefinitions)
            {

                if (!lstSourceDbObjectDefinitions.Exists(objDef => objDef.Key == dbObjectDefinition.Key))
                {
                    index++;
                    // Add object Drop
                    string strObjectFullName = dbObjectDefinition.Value.Substring(12, dbObjectDefinition.Value.IndexOf(' ', 13) - 12);
                    string strObjectType = dbObjectDefinition.Value.Substring(7, 4);
                    if (index != 1)
                    {
                        outfile.WriteLine("GO");
                    }
                    string strDropObjectQuery = "DROP " + strObjectType + " " + strObjectFullName + ";";
                    outfile.WriteLine(strDropObjectQuery);
                    outfile.WriteLine("");
                }
            }

            if (nbObjectDefinitions > 0)
            {
                outfile.WriteLine("/*  ===========        DROP     ###==> End ===============*/\r\n");
            }
            outfile.WriteLine("PRINT 'END'");
            if (outFile != "")
            {
                outfile.Close();
            }
            rdr.Close();
            Console.WriteLine("done");

        }

        private void getDDL(PDWscripter c, string outFile)
        {
            StreamWriter sw = null;
            FileStream fs = null;


            if (outFile != "")
            {
                fs = new FileStream(outFile, FileMode.Create);
                sw = new StreamWriter(fs);
            }


            // get generated statistics
            String description = "-- script generated the : " + String.Format("{0:d/M/yyyy HH:mm:ss}", DateTime.Now);

            //get objects count
            cmd.CommandText = "select count(1) from sys.schemas where name not in ('dbo','sys','INFORMATION_SCHEMA')";
            int schemacount = (int)cmd.ExecuteScalar();

            description += "\r\n-- objects scripted - tables = " + c.dbTables.Count.ToString() + " - schemas = " + schemacount.ToString();
            sw.WriteLine(description);

            String createUseDbTxt = "USE " + c.sourceDb + "\r\nGO\r\n";
            sw.WriteLine(createUseDbTxt);
            Console.Write("DDL>");
            Console.Write("Objects to script - tables = " + c.dbTables.Count.ToString() + " - schemas = " + schemacount.ToString());


            getSchemas(sw, false);

            if (scriptMode == "Delta")
            {
                // Create temporary DEP schema
                createDeployTmpSchemaTxt = "CREATE SCHEMA DEP;\r\nGO\r\n;\r\n";
                sw.WriteLine(createDeployTmpSchemaTxt);
                sw.WriteLine("DECLARE @COLUMNSNAME VARCHAR(8000) \r\n");
            }

            foreach (TableDef t in dbTables)
            {
                c.sourceTable = t.name;
                c.destTable = t.name;
                c.distribution_policy = t.distribution_policy;
                c.getSourceColumns(false, false);
                c.getClusteredIndex(false, false);
                c.getPartitioning(false, false);
                c.getNonclusteredIndexes(false, false);
                c.getStats(false, false);
                c.buildCreateTableText(sw);
            }

            if (scriptMode == "Delta")
            {
                // DROP temporary DEP schema
                dropDeployTmpSchemaTxt = "DROP SCHEMA DEP;\r\nGO\r\n;\r\n";
                sw.WriteLine(dropDeployTmpSchemaTxt);
            }

            if (outFile != "")
            {
                sw.Close();
            }

            Console.WriteLine("done");
        }

        public void compareDDL(PDWscripter cSource, PDWscripter cTarget, string outFile, Boolean SourceFromFile, FilterSettings Filters)
        {
            StreamWriter sw = null;
            FileStream fs = null;


            if (outFile != "")
            {
                fs = new FileStream(outFile, FileMode.Create);
                sw = new StreamWriter(fs);
            }
            String description = "-- script generated the : " + String.Format("{0:d/M/yyyy HH:mm:ss}", DateTime.Now) + "\r\n";
            sw.WriteLine(description);

            Console.Write("CompareDDL>");

            String createUseDbTxt = "USE " + cTarget.sourceDb + "\r\nGO\r\n";
            sw.WriteLine(createUseDbTxt);

            CompareSchemas(sw, cSource, cTarget, SourceFromFile,Filters);

            CompareDbTables(sw, cSource, cTarget, SourceFromFile,Filters);


            if (outFile != "")
            {
                sw.Close();
            }

            Console.WriteLine("done");
        }

        private void getDB(PDWscripter c, string outFile)
        {
            StreamWriter sw = null;
            FileStream fs = null;

            Console.Write("DB>");
            if (outFile != "")
            {
                fs = new FileStream(outFile, FileMode.Create);
                sw = new StreamWriter(fs);
            }

            String createDbTxt = "CREATE DATABASE " + c.sourceDb + "\r\nWITH\r\n(\r\n";
            sw.WriteLine(createDbTxt);

            if (scriptMode == "Delta")
            {
                // Create temporary DEP schema
                createDeployTmpSchemaTxt = "CREATE SCHEMA DEP;\r\nGO\r\n;\r\n";
                sw.WriteLine(createDeployTmpSchemaTxt);
            }

            foreach (TableDef t in dbTables)
            {
                c.sourceTable = t.name;
                c.destTable = t.name;
                c.distribution_policy = t.distribution_policy;
                c.getSourceColumns(false, false);
                c.getClusteredIndex(false, false);
                c.getPartitioning(false, false);
                c.getNonclusteredIndexes(false, false);
                c.buildCreateTableText(sw);
            }

            if (outFile != "")
            {
                sw.Close();
            }
            Console.WriteLine("done");
        }

        public void CompIterateScriptAllTables(PDWscripter cSource, PDWscripter cTarget, string outFile, Boolean SourceFromFile, FilterSettings Filters)
        {
            string outCompDDLFile = outFile + "_COMP_DDL.dsql";
            string outCompDMLFile = outFile + "_COMP_DML.dsql";
            string strWarningFile = outFile + "_COMP_DDL.warn";

            StreamWriter swWarn = null;
            FileStream fs = null;

            cTarget.warningFile = strWarningFile;
            cSource.warningFile = strWarningFile;

            fs = new FileStream(strWarningFile, FileMode.Create);
            swWarn = new StreamWriter(fs);
            swWarn.Close();

            Console.WriteLine("==== Start Comparison ====");

            if ("ALL" == wrkMode || "DDL" == wrkMode)
            {
                cSource.compareDDL(cSource, cTarget, outCompDDLFile, SourceFromFile, Filters);
            }
            if ("ALL" == wrkMode || "DML" == wrkMode)
            {
                cSource.compareDML(cSource, cTarget, outCompDMLFile, SourceFromFile,Filters);
            }
            Console.WriteLine("==== End Comparison ====");

        }

        public void IterateScriptAllTables(PDWscripter c, string outFile)
        {

            string outDDLFile = outFile + "_DDL.dsql";
            string outDMLFile = outFile + "_DML.dsql";

            if ("ALL" == wrkMode || "DDL" == wrkMode)
            {
                c.getDDL(c, outDDLFile);
            }
            if ("ALL" == wrkMode || "DML" == wrkMode)
            {
                c.getDML(c, outDMLFile);
            }


        }

        public void GetDDLstructureFromJSONfile(string DDLJsonStructureFile)
        {
            using (StreamReader file = File.OpenText(DDLJsonStructureFile))
            {
                JsonSerializer serializer = new JsonSerializer();
                this.dbstruct = (DBStruct)serializer.Deserialize(file, typeof(DBStruct));
            }

            foreach (TableSt tbl in this.dbstruct.tables)
            {
                this.dbTables.Add(new TableDef(tbl.name, tbl.schema, tbl.distribution_policy));
            }
        }

        public void GetDMLstructureFromJSONfile(string DMLJsonStructureFile)
        {
            using (StreamReader file = File.OpenText(DMLJsonStructureFile))
            {
                JsonSerializer serializer = new JsonSerializer();
                this.DbObjectDefinitions = new List<KeyValuePair<string, string>>();
                this.DbObjectDefinitions.AddRange((List<KeyValuePair<String, String>>)serializer.Deserialize(file, typeof(List<KeyValuePair<String, String>>)));
            }
        }

        public string GenerateTableCreateScript (TableSt tbl)
        {
                this.sourceTable = tbl.name;
                this.destTable = tbl.name;
                this.distribution_policy = tbl.distribution_policy;
                this.getSourceColumns(false, true);
                this.getClusteredIndex(false, true);
                this.getPartitioning(false, true);
                this.getNonclusteredIndexes(false, true);
                this.getStats(false, true);
                 return this.buildScriptCreateTable();
        }

    }

}
