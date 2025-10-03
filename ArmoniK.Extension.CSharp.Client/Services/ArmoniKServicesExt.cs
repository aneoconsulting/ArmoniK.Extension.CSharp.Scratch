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
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Extension.CSharp.Client.Common.Domain.Blob;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Session;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Task;
using ArmoniK.Extension.CSharp.Client.Common.Services;
using ArmoniK.Extension.CSharp.Client.Library;

namespace ArmoniK.Extension.CSharp.Client.Services;

/// <summary>
///   Provides extension methods for handling dynamic library usage on ArmoniK's environment.
///   These methods facilitate session management, dynamic library uploading, and task submissions using dynamic libraries.
/// </summary>
public static class ArmoniKServicesExt
{
  /// <summary>
  ///   Creates a session and returns <see cref="SessionInfo" />, the session information.
  /// </summary>
  /// <param name="sessionService">An instance of session service.</param>
  /// <param name="partitionIds">Partitions related to the opened session</param>
  /// <param name="dynamicLibraries">A collection of dynamic libraries that the session will handle.</param>
  /// <param name="taskOptions">Default taskOptions of the session</param>
  /// <returns>
  ///   A task that represents the asynchronous creation of a session.
  /// </returns>
  public static async Task<SessionInfo> CreateSessionWithDllAsync(this ISessionService        sessionService,
                                                                  TaskConfiguration           taskOptions,
                                                                  IEnumerable<string>         partitionIds,
                                                                  IEnumerable<DynamicLibrary> dynamicLibraries)
    => await sessionService.CreateSessionAsync(new DllTasksConfiguration(dynamicLibraries,
                                                                         taskOptions),
                                               partitionIds)
                           .ConfigureAwait(false);

  /// <summary>
  ///   Asynchronously sends a dynamic library blob to a blob service
  /// </summary>
  /// <param name="blobService">The blob service to use for uploading the library.</param>
  /// <param name="session">The session information associated with the blob upload.</param>
  /// <param name="dynamicLibrary">The dynamic library related to the blob being sent.</param>
  /// <param name="content">The binary content of the dynamic library to upload.</param>
  /// <param name="manualDeletion">Whether the blob should be deleted manually.</param>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>
  ///   The created <see cref="DllBlob" /> instance with relevant identifiers.
  /// </returns>
  public static async Task<DllBlob> SendDllBlobAsync(this IBlobService    blobService,
                                                     SessionInfo          session,
                                                     DynamicLibrary       dynamicLibrary,
                                                     ReadOnlyMemory<byte> content,
                                                     bool                 manualDeletion,
                                                     CancellationToken    cancellationToken)
  {
    var blobInfo = await blobService.CreateBlobAsync(session,
                                                     dynamicLibrary.ToString(),
                                                     content,
                                                     manualDeletion,
                                                     cancellationToken)
                                    .ConfigureAwait(false);
    dynamicLibrary.LibraryBlobId = blobInfo.BlobId;
    return new DllBlob(dynamicLibrary)
           {
             BlobId    = blobInfo.BlobId,
             SessionId = session.SessionId,
           };
  }

  /// <summary>
  ///   Asynchronously sends a dynamic library blob to a blob service
  /// </summary>
  /// <param name="blobService">The blob service to use for uploading the library.</param>
  /// <param name="session">The session information associated with the blob upload.</param>
  /// <param name="dynamicLibrary">The dynamic library related to the blob being sent.</param>
  /// <param name="zipPath">File path to the zipped library.</param>
  /// <param name="manualDeletion">Whether the blob should be deleted manually.</param>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>
  ///   The created <see cref="DllBlob" /> instance with relevant identifiers.
  /// </returns>
  public static async Task<DllBlob> SendDllBlobAsync(this IBlobService blobService,
                                                     SessionInfo       session,
                                                     DynamicLibrary    dynamicLibrary,
                                                     string            zipPath,
                                                     bool              manualDeletion,
                                                     CancellationToken cancellationToken)
  {
    var content = File.ReadAllBytes(zipPath);
    return await SendDllBlobAsync(blobService,
                                  session,
                                  dynamicLibrary,
                                  content,
                                  manualDeletion,
                                  cancellationToken)
             .ConfigureAwait(false);
  }

  /// <summary>
  ///   Submits tasks with task service, depending on a previously uploaded dynamic library blob.
  /// </summary>
  /// <param name="taskService">The service responsible for handling task submissions.</param>
  /// <param name="session">Session which must contain the tasks.</param>
  /// <param name="taskNodes">The collection of tasks to submit.</param>
  /// <param name="dllBlob">The dynamic library blob dependency for the tasks.</param>
  /// <param name="manualDeletion">Whether the blob should be deleted manually.</param>
  /// <param name="cancellationToken">A token to monitor for cancellation requests during the task submission process.</param>
  public static async Task<ICollection<TaskInfos>> SubmitTasksWithDllAsync(this ITasksService       taskService,
                                                                           SessionInfo              session,
                                                                           IEnumerable<TaskNodeExt> taskNodes,
                                                                           DllBlob                  dllBlob,
                                                                           bool                     manualDeletion,
                                                                           CancellationToken        cancellationToken)
  {
    taskNodes = taskNodes.Select(x =>
                                 {
                                   x.DataDependencies.Add(dllBlob);

                                   //avoid injection of dlls which were already defined in the session taskOptions
                                   x.TaskOptions.Options.Remove(dllBlob.BlobName);

                                   x.TaskOptions.AddTaskLibraryDefinition(x.DynamicLibrary);
                                   return x;
                                 });

    var result = await taskService.SubmitTasksAsync(session,
                                                    taskNodes,
                                                    manualDeletion,
                                                    cancellationToken)
                                  .ConfigureAwait(false);
    return result;
  }
}

public record TaskNodeExt : TaskNode
{
  public TaskLibraryDefinition DynamicLibrary { get; init; }
}
