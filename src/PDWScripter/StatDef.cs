// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DWScripter
{
    public class StatDef
    {
        public string name;
        public List<StatColumnDef> cols;

        public string filter;

        public StatDef(string name)
        {
            this.name = name;
            cols = new List<StatColumnDef>();
        }
        public virtual int CompareTo(object obj)
        {
            if (obj == null) return 1;
            StatDef otherStatDef = obj as StatDef;
            if (otherStatDef != null)
            {
                if (this == null || otherStatDef == null) return 1;
                if (this.name.ToUpper().CompareTo(otherStatDef.name.ToUpper()) != 0) return 1;
                this.cols.Sort((a, b) => a.name.ToUpper().CompareTo(b.name));
            }
            return 0;
        }
        public Boolean ContainsColumn(string ColumnName)
        {
            return this.cols.Exists(delegate (StatColumnDef e) { return e.name == ColumnName; });
        }

        public Boolean IsFilteredStat()
        {
            return (this.filter != null);
        }

    }
}
