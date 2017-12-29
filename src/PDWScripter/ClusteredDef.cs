// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DWScripter
{
    public class ClusteredDef : List<IndexColumnDef>
    {
        public virtual int CompareTo(object obj)
        {
            if (obj == null) return 1;
            ClusteredDef otherclusteredCols = obj as ClusteredDef;
            if (otherclusteredCols != null)
            {
                if (this == null || otherclusteredCols == null) return 1;
                if (this.Count == 0 && otherclusteredCols.Count == 0) return 0;
                if (this.Count != otherclusteredCols.Count) return 1;
                if (this[0].index_type != otherclusteredCols[0].index_type) return 1;
                this.Sort((a, b) => a.name.ToUpper().CompareTo(b.name));
                otherclusteredCols.Sort((a, b) => a.name.ToUpper().CompareTo(b.name));
                for (int i = 0; i < this.Count; i++)
                {
                    if (this[i].CompareTo(otherclusteredCols[i]) == 1) return 1;
                }
            }
            return 0;
        }
    }
}
