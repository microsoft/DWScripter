// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DWScripter
{
    public class NonclusteredIndexes : List<NonclusteredIndexDef>
    {

        public virtual int CompareTo(object obj)
        {
            if (obj == null) return 1;
            NonclusteredIndexes otherNonclusteredIndexes = obj as NonclusteredIndexes;
            if (otherNonclusteredIndexes != null)
            {
                if (this == null || otherNonclusteredIndexes == null) return 1;
                this.Sort((a, b) => a.name.ToUpper().CompareTo(b.name));
                otherNonclusteredIndexes.Sort((a, b) => a.name.ToUpper().CompareTo(b.name));
                if (this.Count != otherNonclusteredIndexes.Count) return 1;

                for (int i = 0; i < this.Count; i++)
                {
                    if (this[i].CompareTo(otherNonclusteredIndexes[i]) == 1) return 1;
                }
            }
            return 0;
        }

        public NonclusteredIndexDef GetIndex(string IndexName)
        {
            return this.Find(delegate (NonclusteredIndexDef e) { return e.name == IndexName; });
        }

    }
}
