// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DWScripter
{
    public class NonclusteredIndexDef : IComparable
    {
        public string name;
        public List<IndexColumnDef> cols;

        public NonclusteredIndexDef(string name)
        {
            this.name = name;
            cols = new List<IndexColumnDef>();
        }

        public virtual int CompareTo(object obj)
        {
            if (obj == null) return 1;
            NonclusteredIndexDef otherNonclusteredIndexDef = obj as NonclusteredIndexDef;
            if (otherNonclusteredIndexDef != null)
            {
                if (this == null || otherNonclusteredIndexDef == null) return 1;
                if (this.name.ToUpper().CompareTo(otherNonclusteredIndexDef.name.ToUpper()) != 0) return 1;
                this.cols.Sort((a, b) => a.name.ToUpper().CompareTo(b.name));
            }
            return 0;
        }

    }
}
