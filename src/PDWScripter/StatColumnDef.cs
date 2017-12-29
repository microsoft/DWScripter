// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DWScripter
{
    public class StatColumnDef : IComparable
    {
        public int key_ordinal;
        public string name;

        public StatColumnDef(int key_ordinal, string name)
        {
            this.key_ordinal = key_ordinal;
            this.name = name;
        }

        public virtual int CompareTo(object obj)
        {
            if (obj == null) return 1;
            StatColumnDef otherStatColumnDef = obj as StatColumnDef;
            if (otherStatColumnDef != null)
            {
                if (this == null || otherStatColumnDef == null) return 1;
                if (this.key_ordinal.CompareTo(otherStatColumnDef.key_ordinal) != 0) return 1;
                if (this.name.ToUpper().CompareTo(otherStatColumnDef.name.ToUpper()) != 0) return 1;
            }
            return 0;
        }
    }
}
