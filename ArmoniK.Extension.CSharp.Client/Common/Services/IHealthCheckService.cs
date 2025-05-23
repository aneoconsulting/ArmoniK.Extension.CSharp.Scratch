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

using System.Collections.Generic;
using System.Threading;

using ArmoniK.Extension.CSharp.Client.Common.Domain.Health;

namespace ArmoniK.Extension.CSharp.Client.Common.Services;

/// <summary>
///   Defines a service for performing health checks on the system components.
/// </summary>
public interface IHealthCheckService
{
  /// <summary>
  ///   Asynchronously retrieves the health status of the system components.
  /// </summary>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>An asynchronous enumerable of health statuses.</returns>
  IAsyncEnumerable<Health> GetHealthAsync(CancellationToken cancellationToken);
}
