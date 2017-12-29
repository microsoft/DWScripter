// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DWScripter
{
    public class IndexColumnDef : IComparable
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

        public virtual int CompareTo(object obj)
        {
            if (obj == null) return 1;
            IndexColumnDef otherIndexColumnDef = obj as IndexColumnDef;
            if (otherIndexColumnDef != null)
            {
                if (this == null || otherIndexColumnDef == null) return 1;
                if (this.key_ordinal.CompareTo(otherIndexColumnDef.key_ordinal) != 0) return 1;
                if (this.name.ToUpper().CompareTo(otherIndexColumnDef.name.ToUpper()) != 0) return 1;
                if (this.is_descending_key.CompareTo(otherIndexColumnDef.is_descending_key) != 0) return 1;
                if (this.index_type.CompareTo(otherIndexColumnDef.index_type) != 0) return 1;
            }
            return 0;
        }
    }
}
