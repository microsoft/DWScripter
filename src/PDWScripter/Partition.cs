// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DWScripter
{
    public class Partition
    {
        public string partitionColumn;
        public string partitionLeftOrRight;
        public List<PartitionBoundary> partitionBoundaries;
        public Partition()
        {
            this.partitionColumn = "";
            this.partitionLeftOrRight = "";
            this.partitionBoundaries = new List<PartitionBoundary>();
        }

    }
    public struct PartitionBoundary
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
}
