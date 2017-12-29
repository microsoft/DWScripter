// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DWScripter
{
    public class DBStruct
    {
        public List<string> schemas = new List<string>();
        public List<TableSt> tables = new List<TableSt>();
        private List<TableSt> _tables = new List<TableSt>();
        public TableSt GetTable(string TableName)
        {
            return this.tables.Find(delegate (TableSt e) { return e.name == TableName; });
        }
        
        public List<TableSt> GetTablesBySchema(List<string> ListSchemaName)
        {

            foreach (string SchemaName in ListSchemaName)
            {
                _tables.AddRange(this.tables.FindAll(delegate (TableSt e) { return e.schema == SchemaName; }));
            }

            return _tables;
        }
    }
}
