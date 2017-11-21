// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.


using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.SqlClient;
using System.Text.RegularExpressions;




namespace DWScripter
{
    class DWScripter
    {
        private SqlConnection conn;
        private SqlCommand cmd;
        private SqlDataReader rdr;
        private List<TableDef> dbTables;
        private List<ColumnDef> cols;
        private string distColumn;
        private List<IndexColumnDef> clusteredCols;
        private List<PartitionBoundary> partitionBoundaries;
        private string createTableTxt;
        private string createSchemaTxt;
        private string sourceDb;
        private string destDb;
        private string sourceTable;
        private string destTable;
        private Int16 distribution_policy;
        private string columnClause;
        private string clusteredClause;
        private string partitionBoundaryClause;
        private string partitionColumn;
        private string partitionLeftOrRight;
        private string filterSpec;
        private string wrkMode;
        private List<NonclusteredIndexDef> nonclusteredIndexes;
        private string nonClusteredClause;
        private List<StatDef> stats;
        private string statsClause;

        private PDWVersionStruct PDWVersion = new PDWVersionStruct();
        //USE PDWVersion to determine backward compatibility
        //APS V2 AU6 - APS2016 RTM version : 13.0.8493.0 
        //      Microsoft Parallel Data Warehouse - 10.0.8493.0 (X64) Nov 2 2016 19:01:10 Copyright (c) Microsoft Corporation Parallel Data Warehouse (64-bit) on Windows NT 6.2 <X64> (Build 9200: )

        private struct PDWVersionStruct
        {
            public int Major;
            public int Minor;
            public int Revision;
            public string Value;
        }


        private struct PartitionBoundary
        {
            public Int32 partition_number;
            public string boundary_value;
            public string boundary_value_type;

            public PartitionBoundary(Int32 partition_number, string boundary_value, string boundary_value_type)
            {
                this.partition_number = partition_number;
                this.boundary_value = boundary_value;
                this.boundary_value_type = boundary_value_type;
            }
        }

        private struct IndexColumnDef
        {
            public byte key_ordinal;
            public string name;
            public bool is_descending_key;
            public byte index_type;

            public IndexColumnDef(byte key_ordinal, string name, bool is_descending_key, byte index_type)
            {
                this.key_ordinal = key_ordinal;
                this.name = name;
                this.is_descending_key = is_descending_key;
                this.index_type = index_type;
            }
        }

        private struct StatColumnDef
        {
            public int key_ordinal;
            public string name;

            public StatColumnDef(int key_ordinal, string name)
            {
                this.key_ordinal = key_ordinal;
                this.name = name;
            }
        }

        private class NonclusteredIndexDef
        {
            public string name;
            public List<IndexColumnDef> cols;

            public NonclusteredIndexDef(string name)
            {
                this.name = name;
                cols = new List<IndexColumnDef>();
            }

        }

        private class StatDef
        {
            public string name;
            public List<StatColumnDef> cols;

            public string filter;

            public StatDef(string name)
            {
                this.name = name;
                cols = new List<StatColumnDef>();
            }

        }

        public struct TableDef
        {
            public string name;
            public byte distribution_policy;

            public TableDef(string name, byte distribution_policy)
            {

                this.name = name;
                this.distribution_policy = distribution_policy;

            }
        }

        public struct ColumnDef
        {
            public Int32 column_id;
            public string name;
            public string type;
            public Int16 max_length;
            public byte precision;
            public byte scale;
            public bool is_nullable;
            public byte distrbution_ordinal;
            public string collation_name;

            public ColumnDef(Int32 column_id, string name, string type, Int16 max_length, byte precision, byte scale, bool is_nullable, byte distribution_ordinal, string collation_name)
            {
                this.column_id = column_id;
                this.name = name;
                this.type = type;
                this.max_length = max_length;
                this.precision = precision;
                this.scale = scale;
                this.is_nullable = is_nullable;
                this.distrbution_ordinal = distribution_ordinal;
                this.collation_name = collation_name;
            }
        }

        public DWScripter(string system, string server, int port, string sourceDb, string userName, string pwd, string wrkMode, string filterSpec)
        {
            cols = new List<ColumnDef>();
            clusteredCols = new List<IndexColumnDef>();
            partitionBoundaries = new List<PartitionBoundary>();
            nonclusteredIndexes = new List<NonclusteredIndexDef>();
            stats = new List<StatDef>();
            dbTables = new List<TableDef>();
            this.filterSpec = filterSpec;
            this.wrkMode = wrkMode;
            this.sourceDb = sourceDb;
            this.destDb = sourceDb;         // For future DB cloning 

            conn = new System.Data.SqlClient.SqlConnection();
            if (system == "APS")
            {
                conn.ConnectionString = "server=" + server + "," + port + ";database=" + sourceDb + ";User ID=" + userName + ";Password=" + pwd;
            }
            else
            {
                conn.ConnectionString = "server=" + server + ";database=" + sourceDb + ";User ID=" + userName + ";Password=" + pwd;
            }

            cmd = new System.Data.SqlClient.SqlCommand();

            conn.Open();
            cmd.Connection = conn;
        }

        private void getSchemas(StreamWriter sw)
        {
            cmd.CommandText =
                "select name from sys.schemas where name not in ('dbo','sys','INFORMATION_SCHEMA')";

            rdr = cmd.ExecuteReader();

            while (rdr.Read())
            {
                createSchemaTxt = "CREATE SCHEMA " + rdr.GetString(rdr.GetOrdinal("name")) + "\r\nGO\r\n;\r\n";
                sw.WriteLine(createSchemaTxt);
            }

            rdr.Close();
        }

        private void getDbTables()
        {
            dbTables.Clear();
            cmd.CommandText =
                "select schema_name(so.schema_id) + '.' + so.name as name, tdp.distribution_policy " +
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

                    dbTables.Add(new TableDef(
                        rdr.GetString(rdr.GetOrdinal("name")),
                        rdr.GetByte(rdr.GetOrdinal("distribution_policy"))
                        ));
                }
            }
            rdr.Close();
        }

        private void getSourceColumns()
        {
            cols.Clear();
            distColumn = "";
            columnClause = "";
            StringBuilder columnspec = new StringBuilder();

            cmd.CommandText =
                "select c.column_id, c.name, t.name as type, c.max_length, c.precision," +
                "c.scale, c.is_nullable, d.distribution_ordinal, c.collation_name " +
                "from sys.columns c " +
                "join sys.pdw_column_distribution_properties d " +
                "on c.object_id = d.object_id and c.column_id = d.column_id " +
                "join sys.types t on t.user_type_id = c.user_type_id " +
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
                        rdr["collation_name"] == DBNull.Value ? string.Empty : (string)rdr["collation_name"]
                        ));
                }

                rdr.Close();
            }



            foreach (ColumnDef c in cols)
            {
                if (c.distrbution_ordinal == 1)
                {
                    // Save name of Distribution column
                    distColumn = c.name;
                }
                if (c.column_id > 1)
                {
                    columnspec.Append("\r\n\t,");
                }
                else
                {
                    columnspec.Append("\t");
                }
                columnspec.Append("[" + c.name + "]" + "\t" + c.type + "\t");
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
                    c.type == "real" ||
                    c.type == "uniqueidentifier")
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
                }

                else if (
                    c.type == "float")
                {
                    // precision only
                    columnspec.Append("(");
                    columnspec.Append(c.precision);
                    columnspec.Append(")\t");
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
                }

                else if (
                    c.type == "decimal")
                {
                    // Precision and Scale
                    columnspec.Append("(");
                    columnspec.Append(c.precision);
                    columnspec.Append(",");
                    columnspec.Append(c.scale);
                    columnspec.Append(")\t");
                }

                else
                {
                    Exception e = new Exception("Unsupported Type " + c.type);
                    throw e;
                }

                columnspec.Append(c.is_nullable ? "NULL" : "NOT NULL");
            }
            columnClause = columnspec.ToString();


            if (cols.Count == 0)
            {
                // invalid query
                throw new Exception("Unable to retrieve column data for " + sourceTable + " in database " + sourceDb);
            }
        }

        private void getClusteredIndex()
        {
            clusteredCols.Clear();
            clusteredClause = "";
            StringBuilder clusteredspec = new StringBuilder();
            Boolean isCCI = false;

            cmd.CommandText =
            "select i.key_ordinal, c.name, i.is_descending_key, si.[type] as index_type " +
            "from sys.indexes si " +
            "left join sys.index_columns i on i.object_id = si.object_id " +
            "left join sys.columns c on c.column_id = i.column_id and c.object_id = i.object_id " +
            "where i.index_id = 1 and si.[type] <> 2 and " +
            "i.object_id = (select object_id from sys.tables where schema_name(schema_id) + '.' + name = '" + sourceTable + "') " +
            "order by key_ordinal ";

            rdr = cmd.ExecuteReader();

            if (rdr.HasRows) 
            {
                while (rdr.Read())
                {
                    clusteredCols.Add(new IndexColumnDef(
                        rdr.GetByte(rdr.GetOrdinal("key_ordinal")),
                        rdr.GetString(rdr.GetOrdinal("name")),
                        rdr.GetBoolean(rdr.GetOrdinal("is_descending_key")),
                        rdr.GetByte(rdr.GetOrdinal("index_type"))
                        ));
                }
            }

            rdr.Close();

            foreach (IndexColumnDef i in clusteredCols)
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

        private void getNonclusteredIndexes()
        {
            nonclusteredIndexes.Clear();
            nonClusteredClause = "";
            StringBuilder nonclusteredspec = new StringBuilder();

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
                    }
                    ncidef.cols.Add(new IndexColumnDef(
                        rdr.GetByte(rdr.GetOrdinal("key_ordinal")),
                        rdr.GetString(rdr.GetOrdinal("name")),
                        rdr.GetBoolean(rdr.GetOrdinal("is_descending_key")),
                        0
                        ));
                }
            }

            rdr.Close();

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
                nonclusteredspec.Append(")\r\n;\r\n");
            }
            nonClusteredClause = nonclusteredspec.ToString();
        }

        private void getStats()
        {
            stats.Clear();
            statsClause = "";
            StringBuilder statspec = new StringBuilder();

            cmd.CommandText =
                "select s.name as stat_name, sc.stats_column_id, c.name, has_filter, filter_definition " +
                "from sys.stats s " +
                "join sys.stats_columns sc on s.stats_id = sc.stats_id and s.object_id = sc.object_id " +
                "join sys.columns c on c.column_id = sc.column_id  and c.object_id = sc.object_id " +
                "where s.object_id = (select object_id from sys.tables where schema_name(schema_id) + '.' + name = '" + sourceTable + "') " +
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
                        stats.Add(statdef);
                        if (!(rdr.IsDBNull(rdr.GetOrdinal("filter_definition"))))
                        statdef.filter = rdr.GetString(rdr.GetOrdinal("filter_definition"));
                    }
                    statdef.cols.Add(new StatColumnDef(
                        rdr.GetInt32(rdr.GetOrdinal("stats_column_id")),
                        rdr.GetString(rdr.GetOrdinal("name"))
                        ));
                    
                }
                
            }

            rdr.Close();

            


            foreach (StatDef statdef in stats)
            {
                statspec.Append("CREATE STATISTICS [" + statdef.name + "] ON [" + destDb + "].[" + destTable.Replace(".", "].[") + "] ");
                foreach (StatColumnDef i in statdef.cols)
                {
                    if (i.key_ordinal > 1)
                    {
                        statspec.Append("\r\n,");
                    }
                    else
                    {
                        statspec.Append("\r\n(");
                    }
                    statspec.Append("[" + i.name + "] ");
                }
                //filtered stats
                statspec.Append(")");
                if (statdef.filter != null)
                {
                    statspec.Append("\r\nWHERE "+statdef.filter);
                }
                statspec.Append("\r\n;\r\n");
            }
            statsClause = statspec.ToString();
        }

        private void getDML(string outFile)
        {
            StreamWriter outfile = null;
            Console.Write("DML>");

            cmd.CommandText = "select definition, object_name(object_id) from sys.sql_modules order by object_id;";
            rdr = cmd.ExecuteReader();

            if (outFile != "")
            {
                outfile = new StreamWriter(outFile, true);
            }
            while (rdr.Read())
            {
                IDataRecord record = (IDataRecord)rdr;
                outfile.WriteLine(String.Format("{0}", record[0]));
                outfile.WriteLine("");
                outfile.WriteLine("go");
                outfile.WriteLine("");
                outfile.WriteLine("");
                outfile.WriteLine("");
                Console.Write(".");
            }
            if (outFile != "")
            {
                outfile.Close();
            }
            rdr.Close();
            Console.WriteLine("done");

        }


        private void getPartitioning()
        {
            partitionColumn = null;
            partitionLeftOrRight = null;
            partitionBoundaryClause = "";
            StringBuilder partitionspec = new StringBuilder();
            partitionBoundaries.Clear();

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

            }
            partitionBoundaryClause = partitionspec.ToString();
        }


        private void buildCreateTableText(StreamWriter sw)
        {
            
            createTableTxt = "CREATE TABLE [" + destDb + "].[" + destTable.Replace(".", "].[") + "]\r\n(\r\n" +
                columnClause + "\r\n)\r\n" +
                "WITH ( DISTRIBUTION = " + (distribution_policy == 2 ? ("HASH ([" + distColumn + "])") : (distribution_policy == 3 ? "REPLICATE" : "ROUND_ROBIN")) +
                
                (clusteredClause != "" ? "\r\n, " + clusteredClause : "") +
                (partitionBoundaryClause != "" ? ("\r\n, PARTITION ([" + partitionColumn + "] RANGE " + partitionLeftOrRight + " FOR VALUES \r\n(" +
                partitionBoundaryClause + ")") : "") +
                ")\r\n;\r\n" + nonClusteredClause + statsClause;

            sw.WriteLine(createTableTxt);
            Console.Write(".");
        }

        private void getDDL(DWScripter c, string outFile)
        {
            StreamWriter sw = null;
            FileStream fs = null;

            Console.Write("DDL>");
            if (outFile != "")
            {
                fs = new FileStream(outFile, FileMode.Create);
                sw = new StreamWriter(fs);
            }

            getSchemas(sw);

            foreach (TableDef t in dbTables)
            {
                Console.WriteLine(t.name);
                c.sourceTable = t.name;
                c.destTable = t.name;
                c.distribution_policy = t.distribution_policy;
                c.getSourceColumns();
                c.getClusteredIndex();
                c.getPartitioning();
                c.getNonclusteredIndexes();
                c.getStats();
                c.buildCreateTableText(sw);
            }

            if (outFile != "")
            {
                sw.Close();
            }
            Console.WriteLine("done");
        }

        private void getVersion()
        {
            string sPDWVersionString;
            string[] VersionMajorMinors;

            cmd.CommandText = "select @@version option(label = 'DWScripter')";
            sPDWVersionString = cmd.ExecuteScalar().ToString();
            PDWVersion.Value = Regex.Match(sPDWVersionString, @"\d+\.\d+\.\d+\.\d").ToString();
            VersionMajorMinors = PDWVersion.Value.Split('.');
            PDWVersion.Major = Int32.Parse(VersionMajorMinors[0]);
            PDWVersion.Minor = Int32.Parse(VersionMajorMinors[1]);
            PDWVersion.Revision = Int32.Parse(VersionMajorMinors[2]);
        }

        static void Main(string[] args)
        {
            

            string server = "";
            string strport = "";
            string sourceDb = "";
            string userName = "";
            string pwd = "";
            string wrkMode = "";
            string filterSpec = "";
            string outFile = "";
            int port = 0;
            string system = "";


            if (args.Count() == 0 || args.Count() < 8)
            {
                Console.WriteLine("USAGE: DWScripter \r\n\t<system> <server> <port> <database> <user> <password> \r\n\t<mode (DDL|DDL|ALL)> <LIKE% filter> <output_file without extension>");
                Console.WriteLine("\r\nEXAMPLE: DWScripter APS 10.36.86.182 17001 dwsys sa <pwd> ALL % pdw_script");
                Console.WriteLine();

                Console.Write("System: (APS or SQLDW) ");
                system = Console.ReadLine().ToUpper();

                Console.Write("Server: (name or ip address) ");
                server = Console.ReadLine();

                //Default port should depend on system.
                if ("APS" == system)
                {
                    Console.Write("Port: (enter= 17001) ");
                    strport = Console.ReadLine();
                    port = (strport == "" ? 17001 : Int16.Parse(strport));
                }
                else if ("SQLDW" == system)
                {
                    Console.Write("Port: (enter= 1433) ");
                    strport = Console.ReadLine();
                    port = (strport == "" ? 1433 : Int16.Parse(strport));
                }


                /*Default database should depend on system
                 * APS has no defaults (can default to dwsys)
                 * SQLDW defaults to databse name derived from server
                */
                if ("APS" == system)
                {
                    Console.Write("Database: (enter= dwsys) ");
                    sourceDb = Console.ReadLine();
                    sourceDb = (sourceDb == "" ? "dwsys" : sourceDb);
                }
                else if ("SQLDW" == system)
                {
                    string DefaultDbName = server.Substring(0, server.IndexOf('.'));
                    Console.Write("Database: (enter= " + DefaultDbName + ") ");
                    sourceDb = Console.ReadLine();
                    sourceDb = (sourceDb == "" ? DefaultDbName : sourceDb);

                }
                

                Console.Write("User Name: (enter= sa) ");
                userName = Console.ReadLine();
                userName = (userName == "" ? "sa" : userName);

                //password management


                Console.Write("Password: ");
                ConsoleColor fg = Console.ForegroundColor;
                Console.ForegroundColor = Console.BackgroundColor;
                Console.CursorVisible = false;
                pwd = Console.ReadLine();
                Console.CursorVisible = true;
                Console.ForegroundColor = fg;

                Console.Write("Mode: DML|DDL|ALL (enter= ALL) ");
                wrkMode = Console.ReadLine().ToUpper();
                wrkMode = (wrkMode == "" ? "ALL" : wrkMode);

                Console.Write("Table Name LIKE Filter: (enter= %) ");
                filterSpec = Console.ReadLine();
                filterSpec = (filterSpec == "" ? "%" : filterSpec);

                Console.Write("Output file name without extension: (enter = " + sourceDb + ") ");
                outFile = Console.ReadLine();
                outFile = (outFile == "" ? sourceDb : outFile);

                Console.WriteLine();

            }
            else
            {
                system = args[0].ToUpper();
                server = args[1];
                port = Int16.Parse(args[2]);
                sourceDb = args[3];
                userName = args[4];
                pwd = args[5];
                wrkMode = args[6].ToUpper();
                filterSpec = args[7];
                outFile = args[8];
            }

            if (wrkMode != "ALL" & wrkMode != "DDL" & wrkMode != "DML")
            {
                Console.WriteLine("Uknown mode. USE: DML|DDL|ALL");
                return;
            }

            DWScripter c = null;
            try
            {
                c = new DWScripter(system, server, port, sourceDb, userName, pwd, wrkMode, filterSpec);
                // c.init(c.conn);
                Console.WriteLine("Connection Opened");
                c.getVersion();
                Console.WriteLine("APS Version : " + c.PDWVersion.Value.ToString());

                c.getDbTables();
                c.IterateScriptAllTables(c, outFile);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                return;
            }

            c.conn.Close();
        }

        private void IterateScriptAllTables(DWScripter c, string outFile)
        {

            string outDDLFile = outFile + "_DDL.sql";
            string outDMLFile = outFile + "_DML.sql";

            if ("ALL" == wrkMode || "DDL" == wrkMode)
            {
                c.getDDL(c, outDDLFile);
            }
            if ("ALL" == wrkMode || "DML" == wrkMode)
            {
                c.getDML(outDMLFile);
            }
        }
    }
}
