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

using System;

namespace ArmoniK.Extension.CSharp.Client.Common.Services;

/// <summary>
///   Defines the set of services used by a ArmoniK client instance
/// </summary>
public interface IServicesConfiguration
{
  /// <summary>
  ///   Returns the function that creates the BlobService
  /// </summary>
  Func<IServiceProvider, IBlobService> BlobServiceBuilder { get; }

  /// <summary>
  ///   Returns the function that creates the EventsService
  /// </summary>
  Func<IServiceProvider, IEventsService> EventsServiceBuilder { get; }

  /// <summary>
  ///   Returns the function that creates the HealthCheckService
  /// </summary>
  Func<IServiceProvider, IHealthCheckService> HealthCheckServiceBuilder { get; }

  /// <summary>
  ///   Returns the function that creates the PartitionsService
  /// </summary>
  Func<IServiceProvider, IPartitionsService> PartitionsServiceBuilder { get; }

  /// <summary>
  ///   Returns the function that creates the SessionService
  /// </summary>
  Func<IServiceProvider, ISessionService> SessionServiceBuilder { get; }

  /// <summary>
  ///   Returns the function that creates the TasksService
  /// </summary>
  Func<IServiceProvider, ITasksService> TaskServiceBuilder { get; }

  /// <summary>
  ///   Returns the function that creates the VersionsService
  /// </summary>
  Func<IServiceProvider, IVersionsService> VersionsServiceBuilder { get; }
}
