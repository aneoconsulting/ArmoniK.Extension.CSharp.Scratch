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

using System.Text;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Agent;
using ArmoniK.Api.Worker.Worker;

namespace ArmoniK.Extension.CSharp.DllCommon;

/// <summary>
///   Allow a worker to create tasks and populate results
/// </summary>
public class UserTaskHandler
{
  private readonly ITaskHandler taskHandler_;

  /// <summary>
  ///   Creates a UserTaskHandler
  /// </summary>
  /// <param name="taskHandler"></param>
  /// <param name="inputs"></param>
  /// <param name="outputs"></param>
  public UserTaskHandler(ITaskHandler                        taskHandler,
                         IReadOnlyDictionary<string, byte[]> inputs,
                         IReadOnlyDictionary<string, string> outputs)
  {
    taskHandler_ = taskHandler;
    Inputs       = inputs;
    Outputs      = outputs;
  }

  /// <summary>Id of the session this task belongs to.</summary>
  public string SessionId
    => taskHandler_.SessionId;

  /// <summary>Task's id being processed.</summary>
  public string TaskId
    => taskHandler_.TaskId;

  /// <summary>List of options provided when submitting the task.</summary>
  public TaskOptions TaskOptions
    => taskHandler_.TaskOptions;

  /// <summary>
  ///   The data required to compute the task. The key is the name defined by the client, the value is the raw data.
  /// </summary>
  public IReadOnlyDictionary<string, byte[]> Inputs { get; }

  /// <summary>
  ///   Result blob ids by name defined by the client.
  /// </summary>
  public IReadOnlyDictionary<string, string> Outputs { get; }

  /// <summary>
  ///   The configuration parameters for the interaction with ArmoniK.
  /// </summary>
  public Configuration Configuration
    => taskHandler_.Configuration;

  /// <summary>
  ///   Decode a dependency from its raw data
  /// </summary>
  /// <param name="name">The input name defined by the client</param>
  /// <returns>The decoded string</returns>
  public string GetStringDependency(string name)
    => Encoding.UTF8.GetString(Inputs[name]);

  /// <summary>
  ///   Decode a dependency from its raw data
  /// </summary>
  /// <param name="name">The input name defined by the client</param>
  /// <returns>The decoded integer</returns>
  public int GetIntDependency(string name)
    => BitConverter.ToInt32(Inputs[name]);

  /// <summary>
  ///   Decode a dependency from its raw data
  /// </summary>
  /// <param name="name">The input name defined by the client</param>
  /// <returns>The decoded double</returns>
  public double GetDoubleDependency(string name)
    => BitConverter.ToDouble(Inputs[name]);

  /// <summary>This method allows to create subtasks.</summary>
  /// <param name="tasks">Lists the tasks to submit</param>
  /// <param name="taskOptions">The task options. If no value is provided, will use the default session options</param>
  /// <param name="cancellationToken">
  ///   Token used to cancel the execution of the method.
  ///   If null, the cancellation token of the task handler is used
  /// </param>
  /// <returns></returns>
  public Task<CreateTaskReply> CreateTasksAsync(IEnumerable<TaskRequest> tasks,
                                                TaskOptions?             taskOptions       = null,
                                                CancellationToken?       cancellationToken = null)
    => taskHandler_.CreateTasksAsync(tasks,
                                     taskOptions,
                                     cancellationToken);

  /// <summary>
  ///   NOT IMPLEMENTED
  ///   This method is used to retrieve data available system-wide.
  /// </summary>
  /// <param name="key">The data key identifier</param>
  /// <param name="cancellationToken">
  ///   Token used to cancel the execution of the method.
  ///   If null, the cancellation token of the task handler is used
  /// </param>
  /// <returns></returns>
  public Task<byte[]> RequestResource(string             key,
                                      CancellationToken? cancellationToken = null)
    => RequestResource(key,
                       cancellationToken);

  /// <summary>
  ///   NOT IMPLEMENTED
  ///   This method is used to retrieve data provided when creating the session.
  /// </summary>
  /// <param name="key">The da ta key identifier</param>
  /// <param name="cancellationToken">
  ///   Token used to cancel the execution of the method.
  ///   If null, the cancellation token of the task handler is used
  /// </param>
  /// <returns></returns>
  public Task<byte[]> RequestCommonData(string             key,
                                        CancellationToken? cancellationToken = null)
    => RequestCommonData(key,
                         cancellationToken);

  /// <summary>
  ///   NOT IMPLEMENTED
  ///   This method is used to retrieve data directly from the submission client.
  /// </summary>
  /// <param name="key"></param>
  /// <param name="cancellationToken">
  ///   Token used to cancel the execution of the method.
  ///   If null, the cancellation token of the task handler is used
  /// </param>
  /// <returns></returns>
  public Task<byte[]> RequestDirectData(string             key,
                                        CancellationToken? cancellationToken = null)
    => taskHandler_.RequestDirectData(key,
                                      cancellationToken);

  /// <summary>Send the results computed by the task</summary>
  /// <param name="key">The key identifier of the result.</param>
  /// <param name="data">The data corresponding to the result</param>
  /// <param name="cancellationToken">
  ///   Token used to cancel the execution of the method.
  ///   If null, the cancellation token of the task handler is used
  /// </param>
  /// <returns></returns>
  public Task SendResult(string             key,
                         byte[]             data,
                         CancellationToken? cancellationToken = null)
    => taskHandler_.SendResult(key,
                               data,
                               cancellationToken);

  /// <summary>Create results metadata</summary>
  /// <param name="results">The collection of results to be created</param>
  /// <param name="cancellationToken">
  ///   Token used to cancel the execution of the method.
  ///   If null, the cancellation token of the task handler is used
  /// </param>
  /// <returns>The result creation response</returns>
  public Task<CreateResultsMetaDataResponse> CreateResultsMetaDataAsync(IEnumerable<CreateResultsMetaDataRequest.Types.ResultCreate> results,
                                                                        CancellationToken?                                           cancellationToken = null)
    => taskHandler_.CreateResultsMetaDataAsync(results,
                                               cancellationToken);

  /// <summary>Submit tasks with existing payloads (results)</summary>
  /// <param name="taskCreations">The requests to create tasks</param>
  /// <param name="submissionTaskOptions">optional tasks for the whole submission</param>
  /// <param name="cancellationToken">
  ///   Token used to cancel the execution of the method.
  ///   If null, the cancellation token of the task handler is used
  /// </param>
  /// <returns>The task submission response</returns>
  public Task<SubmitTasksResponse> SubmitTasksAsync(IEnumerable<SubmitTasksRequest.Types.TaskCreation> taskCreations,
                                                    TaskOptions?                                       submissionTaskOptions,
                                                    CancellationToken?                                 cancellationToken = null)
    => taskHandler_.SubmitTasksAsync(taskCreations,
                                     submissionTaskOptions,
                                     cancellationToken);

  /// <summary>
  ///   Create results from metadata and data in an unique request
  /// </summary>
  /// <param name="results">The results to create</param>
  /// <param name="cancellationToken">
  ///   Token used to cancel the execution of the method.
  ///   If null, the cancellation token of the task handler is used
  /// </param>
  /// <returns>The task submission response</returns>
  public Task<CreateResultsResponse> CreateResultsAsync(IEnumerable<CreateResultsRequest.Types.ResultCreate> results,
                                                        CancellationToken?                                   cancellationToken = null)
    => taskHandler_.CreateResultsAsync(results,
                                       cancellationToken);
}
