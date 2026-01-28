// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2026. All rights reserved.
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

using System.Text;

using ArmoniK.Extensions.CSharp.Common.Common.Domain.Task;
using ArmoniK.Extensions.CSharp.Worker.Interfaces.Common.Domain.Task;
using ArmoniK.Extensions.CSharp.Worker.Interfaces.Handles;

namespace ArmoniK.Extensions.CSharp.Worker.Interfaces;

/// <summary>
///   Allow a worker to create tasks and populate results
/// </summary>
public interface ISdkTaskHandler
{
  /// <summary>Session's id this task belongs to.</summary>
  string SessionId { get; }

  /// <summary>Task's id being processed.</summary>
  string TaskId { get; }

  /// <summary>Configuration settings for the task.</summary>
  TaskConfiguration TaskOptions { get; }

  /// <summary>
  ///   The data required to compute the task. The key is the name defined by the client, the value is the raw data.
  /// </summary>
  public IReadOnlyDictionary<string, BlobHandle> Inputs { get; }

  /// <summary>
  ///   Result blob ids by name defined by the client.
  /// </summary>
  public IReadOnlyDictionary<string, BlobHandle> Outputs { get; }

  /// <summary>
  ///   Decode a dependency from its raw data
  /// </summary>
  /// <param name="name">The input name defined by the client</param>
  /// <param name="encoding">Encoding used for the string, when null UTF-8 is used</param>
  /// <returns>The decoded string</returns>
  string GetStringDependency(string    name,
                             Encoding? encoding = null);

  /// <summary>Send the results computed by the task</summary>
  /// <param name="blob">The blob handle.</param>
  /// <param name="data">The data corresponding to the result</param>
  /// <param name="cancellationToken">
  ///   Token used to cancel the execution of the method.
  ///   If null, the cancellation token of the task handler is used
  /// </param>
  /// <returns>A task representing the asynchronous operation.</returns>
  Task SendResultAsync(BlobHandle         blob,
                       byte[]             data,
                       CancellationToken? cancellationToken = null);

  /// <summary>Send the results computed by the task</summary>
  /// <param name="blob">The blob handle.</param>
  /// <param name="data">The string result</param>
  /// <param name="encoding">Encoding used for the string, when null UTF-8 is used</param>
  /// <param name="cancellationToken">
  ///   Token used to cancel the execution of the method.
  ///   If null, the cancellation token of the task handler is used
  /// </param>
  /// <returns>A task representing the asynchronous operation.</returns>
  Task SendStringResultAsync(BlobHandle         blob,
                             string             data,
                             Encoding?          encoding          = null,
                             CancellationToken? cancellationToken = null);

  /// <summary>
  ///   Create blobs metadata
  /// </summary>
  /// <param name="names">The collection of blob names to be created</param>
  /// <param name="cancellationToken">
  ///   Token used to cancel the execution of the method.
  ///   If null, the cancellation token of the task handler is used
  /// </param>
  /// <returns>A BlobHandle collection</returns>
  Task<ICollection<BlobHandle>> CreateBlobsMetaDataAsync(IEnumerable<string> names,
                                                         CancellationToken   cancellationToken = default);

  /// <summary>
  ///   Create blobs with their data in a single request
  /// </summary>
  /// <param name="blobs">The blob names and data</param>
  /// <param name="cancellationToken">
  ///   Token used to cancel the execution of the method.
  ///   If null, the cancellation token of the task handler is used
  /// </param>
  /// <returns>A BlobHandle collection</returns>
  Task<ICollection<BlobHandle>> CreateBlobsAsync(IEnumerable<KeyValuePair<string, ReadOnlyMemory<byte>>> blobs,
                                                 CancellationToken                                       cancellationToken = default);

  /// <summary>Submit tasks with existing payloads (results)</summary>
  /// <param name="taskDefinitions">The requests to create tasks</param>
  /// <param name="submissionTaskOptions">optional tasks for the whole submission</param>
  /// <param name="cancellationToken">
  ///   Token used to cancel the execution of the method.
  ///   If null, the cancellation token of the task handler is used
  /// </param>
  /// <returns>An asynchronous enumerable of task infos.</returns>
  IAsyncEnumerable<TaskInfos> SubmitTasksAsync(ICollection<TaskDefinition> taskDefinitions,
                                               TaskConfiguration           submissionTaskOptions,
                                               CancellationToken           cancellationToken = default);
}
