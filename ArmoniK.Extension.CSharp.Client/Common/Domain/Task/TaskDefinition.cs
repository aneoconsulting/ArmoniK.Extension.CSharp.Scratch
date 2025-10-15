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

using ArmoniK.Extension.CSharp.Client.Common.Domain.Blob;
using ArmoniK.Extension.CSharp.Client.Handles;
using ArmoniK.Extension.CSharp.Client.Library;

namespace ArmoniK.Extension.CSharp.Client.Common.Domain.Task;

/// <summary>
///   Defines a Task with its inputs and outputs, and submit it.
/// </summary>
public class TaskDefinition
{
  /// <summary>
  ///   Input Blobs to be created
  /// </summary>
  public Dictionary<string, BlobDefinition> InputDefinitions { get; } = new();

  /// <summary>
  ///   Input Blobs already created
  /// </summary>
  public Dictionary<string, BlobHandle> InputHandles { get; } = new();

  /// <summary>
  ///   Output blobs
  /// </summary>
  public Dictionary<string, BlobDefinition> Outputs { get; } = new();

  /// <summary>
  ///   Task options
  /// </summary>
  public TaskConfiguration? TaskOptions { get; internal set; }

  /// <summary>
  ///   The library that implements the task
  /// </summary>
  public DynamicLibrary? WorkerLibrary { get; private set; }

  /// <summary>
  ///   The payload BlobInfo
  /// </summary>
  internal BlobInfo? Payload { get; set; }

  /// <summary>
  ///   Set the worker library information.
  ///   Mandatory when ArmoniK SDK is used on worker side.
  /// </summary>
  /// <param name="workerLibrary">The worker dynamic library</param>
  /// <returns>The TaskDefinition updated</returns>
  public TaskDefinition WithLibrary(DynamicLibrary workerLibrary)
  {
    WorkerLibrary = workerLibrary;
    return this;
  }

  /// <summary>
  ///   Add an input blob when the blob was not already created
  /// </summary>
  /// <param name="name">The name of the input blob</param>
  /// <param name="blobDeclaration">The blob definition</param>
  /// <returns>The TaskDefinition updated</returns>
  public TaskDefinition WithInput(string         name,
                                  BlobDefinition blobDeclaration)
  {
    InputDefinitions.Add(name,
                         blobDeclaration);
    return this;
  }

  /// <summary>
  ///   Add an input blob when the blob was already created
  /// </summary>
  /// <param name="name">The name of the input blob</param>
  /// <param name="blobHandle">The blob handle</param>
  /// <returns>The TaskDefinition updated</returns>
  public TaskDefinition WithInput(string     name,
                                  BlobHandle blobHandle)
  {
    InputHandles.Add(name,
                     blobHandle);
    return this;
  }

  /// <summary>
  ///   Add a new output to the task
  /// </summary>
  /// <param name="outputName">The name of the output blob</param>
  /// <param name="manualDeletion">Whether the blob should be manually deleted</param>
  /// <returns>The TaskDefinition updated</returns>
  public TaskDefinition WithOutput(string outputName,
                                   bool   manualDeletion = false)
  {
    var blobDefinition = BlobDefinition.CreateOutputBlobDefinition(manualDeletion);
    Outputs.Add(outputName,
                blobDefinition);
    return this;
  }

  /// <summary>
  ///   Add specific TaskOption to the task
  /// </summary>
  /// <param name="taskOptions">The task options</param>
  /// <returns>The TaskDefinition updated</returns>
  public TaskDefinition WithTaskOptions(TaskConfiguration taskOptions)
  {
    TaskOptions = taskOptions;
    return this;
  }
}
