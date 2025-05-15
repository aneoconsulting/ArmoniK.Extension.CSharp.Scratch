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

using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Extension.CSharp.Client.Common.Domain.Versions;

namespace ArmoniK.Extension.CSharp.Client.Common.Services;

/// <summary>
///   Defines a service for retrieving version information.
/// </summary>
public interface IVersionsService
{
  /// <summary>
  ///   Asynchronously retrieves version information.
  /// </summary>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>A task representing the asynchronous operation. The task result contains the version information.</returns>
  Task<VersionsInfo> GetVersionAsync(CancellationToken cancellationToken);
}
