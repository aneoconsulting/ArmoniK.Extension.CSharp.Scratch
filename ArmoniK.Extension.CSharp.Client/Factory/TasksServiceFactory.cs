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

using ArmoniK.Extension.CSharp.Client.Common.Services;
using ArmoniK.Extension.CSharp.Client.Services;
using ArmoniK.Utils;

using Grpc.Core;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Extension.CSharp.Client.Factory;

/// <summary>
///   Provides a factory method to create instances of the task service.
/// </summary>
public static class TasksServiceFactory
{
  /// <summary>
  ///   Creates an instance of <see cref="ITasksService" /> using the specified GRPC channel, blob service, and an optional
  ///   logger factory.
  /// </summary>
  /// <param name="channel">An object pool that manages GRPC channels. This provides efficient handling of channel resources.</param>
  /// <param name="blobService">The blob service to be used for blob manipulation operations within the task service.</param>
  /// <param name="loggerFactory">
  ///   An optional logger factory to enable logging within the task service. If null, logging will
  ///   be disabled.
  /// </param>
  /// <returns>An instance of <see cref="ITasksService" /> that can be used to perform task operations.</returns>
  public static ITasksService CreateTaskService(ObjectPool<ChannelBase> channel,
                                                IBlobService            blobService,
                                                ILoggerFactory          loggerFactory = null)
    => new TasksService(channel,
                        blobService,
                        loggerFactory);
}
