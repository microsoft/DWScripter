// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DWScripter
{
    public class TableSt : IComparable
    {
        public string name;
        public string schema;
        public byte distribution_policy;
        public List<ColumnDef> Columns;
        public string ClusteredIndexName;
        public ClusteredDef clusteredcols;
        public NonclusteredIndexes nonclusteredIndexes;
        public Statistics statistics;
        public Partition DBPartition;

        public TableSt ()
        {
            this.name = "";
            this.schema = "";
            this.distribution_policy = 1;
            this.Columns = new List<ColumnDef>();
            this.clusteredcols = new ClusteredDef();
            this.nonclusteredIndexes = new NonclusteredIndexes();
            this.statistics = new Statistics();
            this.DBPartition = new Partition();
            this.ClusteredIndexName = string.Empty;
        }

        public TableSt(string name, string schema, byte distribution_policy)
        {
            this.name = name;
            this.schema = schema;
            this.distribution_policy = distribution_policy;
            this.Columns = new List<ColumnDef>();
            this.clusteredcols = new ClusteredDef();
            this.nonclusteredIndexes = new NonclusteredIndexes();
            this.statistics = new Statistics();
            this.DBPartition = new Partition();
            this.ClusteredIndexName = string.Empty;
        }



        public virtual Boolean ColumnEquals (object obj)
        {
            if (obj == null) return false;
            List<ColumnDef> otherColumns = obj as List<ColumnDef>;
            if (otherColumns != null)
            {
                if (this == null || otherColumns == null) return false;
                if (this.Columns.Count != otherColumns.Count) return false;
                this.Columns.Sort((a, b) => a.name.ToUpper().CompareTo(b.name));
                otherColumns.Sort((a, b) => a.name.ToUpper().CompareTo(b.name));
                
                for (int i = 0; i < this.Columns.Count; i++)
                {
                    if (this.Columns[i].column_id != otherColumns[i].column_id) return false;
                    if (this.Columns[i].name != otherColumns[i].name) return false;
                    if (this.Columns[i].type != otherColumns[i].type) return false;
                    if (this.Columns[i].max_length != otherColumns[i].max_length) return false;
                    if (this.Columns[i].precision != otherColumns[i].precision) return false;
                    if (this.Columns[i].scale != otherColumns[i].scale) return false;
                    if (this.Columns[i].is_nullable != otherColumns[i].is_nullable) return false;
                    if (this.Columns[i].distrbution_ordinal != otherColumns[i].distrbution_ordinal) return false;
                    if (this.Columns[i].collation_name != otherColumns[i].collation_name) return false;
                    if (this.Columns[i].defaultconstraint != otherColumns[i].defaultconstraint) return false;
                }
            }
            return true;
        }
        public virtual int CompareTo(object obj)
        {
            if (obj == null) return 1;
            TableSt otherTable = obj as TableSt;
            if (otherTable != null)
            {
                if (this == null || otherTable == null) return 1;
                if (this.distribution_policy != otherTable.distribution_policy) return 1;
                if (this.nonclusteredIndexes.CompareTo(otherTable.nonclusteredIndexes) == 1) return 1;
                if (this.clusteredcols.CompareTo(otherTable.clusteredcols) == 1) return 1;
                if (this.statistics.Count != otherTable.statistics.Count) return 1;
                if (this.statistics.CompareTo(otherTable.statistics) == 1) return 1;
                if (this.Columns.Count != otherTable.Columns.Count) return 1;
                this.Columns.Sort((a, b) => a.name.ToUpper().CompareTo(b.name));
                otherTable.Columns.Sort((a, b) => a.name.ToUpper().CompareTo(b.name));
                for (int i = 0; i < this.Columns.Count; i++)
                {
                    if (this.Columns[i].column_id != otherTable.Columns[i].column_id) return 1;
                    if (this.Columns[i].name != otherTable.Columns[i].name) return 1;
                    if (this.Columns[i].type != otherTable.Columns[i].type) return 1;
                    if (this.Columns[i].max_length != otherTable.Columns[i].max_length) return 1;
                    if (this.Columns[i].precision != otherTable.Columns[i].precision) return 1;
                    if (this.Columns[i].scale != otherTable.Columns[i].scale) return 1;
                    if (this.Columns[i].is_nullable != otherTable.Columns[i].is_nullable) return 1;
                    if (this.Columns[i].distrbution_ordinal != otherTable.Columns[i].distrbution_ordinal) return 1;
                    if (this.Columns[i].collation_name != otherTable.Columns[i].collation_name) return 1;
                    if (this.Columns[i].defaultconstraint != otherTable.Columns[i].defaultconstraint) return 1;
                }

            }

            return 0;
        }

        public NonclusteredIndexDef GetIndex(string IndexName)
        {
            return this.nonclusteredIndexes.Find(delegate (NonclusteredIndexDef e) { return e.name == IndexName; });
        }
        public string GetDistributionColumn()
        {
            return (this.Columns.Find(delegate (ColumnDef e) { return e.distrbution_ordinal == 1; })).name;
        }
    }
}
