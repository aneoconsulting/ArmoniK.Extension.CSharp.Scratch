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

using ArmoniK.Extension.CSharp.Client.Common;
using ArmoniK.Extension.CSharp.Client.Common.Services;
using ArmoniK.Utils;

using Grpc.Core;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Extension.CSharp.Client;

/// <summary>
///   Defines the interface for use of a ArmoniK client
/// </summary>
public interface IArmoniKClient
{
  /// <summary>
  ///   The properties
  /// </summary>
  Properties Properties { get; }

  /// <summary>
  ///   The logger factory
  /// </summary>
  ILoggerFactory LoggerFactory { get; }

  /// <summary>
  ///   Gets the channel pool used for managing GRPC channels.
  /// </summary>
  ObjectPool<ChannelBase> ChannelPool { get; }

  /// <summary>
  ///   Gets the blob service
  /// </summary>
  IBlobService BlobService { get; }

  /// <summary>
  ///   Gets the tasks service.
  /// </summary>
  ITasksService TasksService { get; }

  /// <summary>
  ///   Gets the session service.
  /// </summary>
  ISessionService SessionService { get; }

  /// <summary>
  ///   Gets the events service.
  /// </summary>
  IEventsService EventsService { get; }

  /// <summary>
  ///   Gets the version service.
  /// </summary>
  IVersionsService VersionService { get; }

  /// <summary>
  ///   Gets the partitions service.
  /// </summary>
  IPartitionsService PartitionsService { get; }

  /// <summary>
  ///   Gets the health check service.
  /// </summary>
  IHealthCheckService HealthCheckService { get; }
}
