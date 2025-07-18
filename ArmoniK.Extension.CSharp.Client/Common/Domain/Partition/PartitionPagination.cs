// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2025. All rights reserved.
// 
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using ArmoniK.Api.gRPC.V1.Partitions;
using ArmoniK.Extension.CSharp.Client.Common.Generic;

namespace ArmoniK.Extension.CSharp.Client.Common.Domain.Partition;

/// <summary>
///   Provides pagination capabilities for partition listings, including sorting and filtering functionalities.
/// </summary>
public record PartitionPagination : Pagination<Filters, PartitionRawEnumField>
{
}
