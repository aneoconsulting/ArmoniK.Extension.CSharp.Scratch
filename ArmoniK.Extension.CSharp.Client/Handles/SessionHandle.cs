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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Extension.CSharp.Client.Common.Domain.Blob;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Session;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Task;
using ArmoniK.Extension.CSharp.Client.Exceptions;
using ArmoniK.Extension.CSharp.Client.Library;
using ArmoniK.Extension.CSharp.Client.Services;
using ArmoniK.Utils;

namespace ArmoniK.Extension.CSharp.Client.Handles;

/// <summary>
///   Handles session management operations for an ArmoniK client, providing methods to control the lifecycle of a
///   session.
/// </summary>
public class SessionHandle : IAsyncDisposable, IDisposable
{
  private readonly ArmoniKClient   armoniKClient_;
  private readonly bool            closeOnDispose_;
  private readonly SessionInfo     sessionInfo_;
  private          CallbackRunner? callbackRunner_;
  private          bool            isDisposed_;

  /// <summary>
  ///   Initializes a new instance of the <see cref="SessionHandle" /> class.
  /// </summary>
  /// <param name="session">The session information for managing session-related operations.</param>
  /// <param name="armoniKClient">The ArmoniK client used to perform operations on the session.</param>
  /// <param name="closeOnDispose">Whether the session should be closed once the SessionHandle instance is disposed.</param>
  internal SessionHandle(SessionInfo   session,
                         ArmoniKClient armoniKClient,
                         bool          closeOnDispose = false)
  {
    armoniKClient_  = armoniKClient ?? throw new ArgumentNullException(nameof(armoniKClient));
    sessionInfo_    = session       ?? throw new ArgumentNullException(nameof(session));
    closeOnDispose_ = closeOnDispose;
  }

  /// <summary>
  ///   Dispose the resources of the session
  /// </summary>
  public async ValueTask DisposeAsync()
  {
    if (!isDisposed_)
    {
      await AbortCallbacksAsync()
        .ConfigureAwait(false);
      if (closeOnDispose_)
      {
        await CloseSessionAsync(CancellationToken.None)
          .ConfigureAwait(false);
      }

      isDisposed_ = true;
    }
  }

  /// <summary>
  ///   Dispose the resources of the session
  /// </summary>
  public void Dispose()
    => DisposeAsync()
      .WaitSync();

  /// <summary>
  ///   Implicit conversion operator from SessionHandle to SessionInfo.
  ///   Allows SessionHandle to be used wherever SessionInfo is expected.
  /// </summary>
  /// <param name="sessionHandle">The SessionHandle to convert.</param>
  /// <returns>The SessionInfo contained within the SessionHandle.</returns>
  /// <exception cref="ArgumentNullException">Thrown when sessionHandle is null.</exception>
  public static implicit operator SessionInfo(SessionHandle sessionHandle)
    => sessionHandle?.sessionInfo_ ?? throw new ArgumentNullException(nameof(sessionHandle));

  /// <summary>
  ///   Cancels the session asynchronously.
  /// </summary>
  /// <param name="cancellationToken">A token that allows processing to be cancelled.</param>
  public async Task CancelSessionAsync(CancellationToken cancellationToken)
    => await armoniKClient_.SessionService.CancelSessionAsync(sessionInfo_,
                                                              cancellationToken)
                           .ConfigureAwait(false);

  /// <summary>
  ///   Closes the session asynchronously.
  /// </summary>
  /// <param name="cancellationToken">A token that allows processing to be cancelled.</param>
  public async Task CloseSessionAsync(CancellationToken cancellationToken)
    => await armoniKClient_.SessionService.CloseSessionAsync(sessionInfo_,
                                                             cancellationToken)
                           .ConfigureAwait(false);

  /// <summary>
  ///   Pauses the session asynchronously.
  /// </summary>
  /// <param name="cancellationToken">A token that allows processing to be cancelled.</param>
  public async Task PauseSessionAsync(CancellationToken cancellationToken)
    => await armoniKClient_.SessionService.PauseSessionAsync(sessionInfo_,
                                                             cancellationToken)
                           .ConfigureAwait(false);

  /// <summary>
  ///   Stops submissions to the session asynchronously.
  /// </summary>
  /// <param name="cancellationToken">A token that allows processing to be cancelled.</param>
  public async Task StopSubmissionAsync(CancellationToken cancellationToken)
    => await armoniKClient_.SessionService.StopSubmissionAsync(sessionInfo_,
                                                               cancellationToken)
                           .ConfigureAwait(false);

  /// <summary>
  ///   Resumes the session asynchronously.
  /// </summary>
  /// <param name="cancellationToken">A token that allows processing to be cancelled.</param>
  public async Task ResumeSessionAsync(CancellationToken cancellationToken)
    => await armoniKClient_.SessionService.ResumeSessionAsync(sessionInfo_,
                                                              cancellationToken)
                           .ConfigureAwait(false);

  /// <summary>
  ///   Purges the session asynchronously, removing all data associated with it.
  /// </summary>
  /// <param name="cancellationToken">A token that allows processing to be cancelled.</param>
  public async Task PurgeSessionAsync(CancellationToken cancellationToken)
    => await armoniKClient_.SessionService.PurgeSessionAsync(sessionInfo_,
                                                             cancellationToken)
                           .ConfigureAwait(false);

  /// <summary>
  ///   Deletes the session asynchronously.
  /// </summary>
  /// <param name="cancellationToken">A token that allows processing to be cancelled.</param>
  public async Task DeleteSessionAsync(CancellationToken cancellationToken)
    => await armoniKClient_.SessionService.DeleteSessionAsync(sessionInfo_,
                                                              cancellationToken)
                           .ConfigureAwait(false);

  /// <summary>
  ///   Asynchronously sends a dynamic library blob to a blob service
  /// </summary>
  /// <param name="dynamicLibrary">The dynamic library related to the blob being sent.</param>
  /// <param name="zipPath">File path to the zipped library.</param>
  /// <param name="manualDeletion">Whether the blob should be deleted manually.</param>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>
  ///   The created <see cref="DllBlob" /> instance with relevant identifiers.
  /// </returns>
  public async Task<DllBlob> SendDllBlobAsync(DynamicLibrary    dynamicLibrary,
                                              string            zipPath,
                                              bool              manualDeletion,
                                              CancellationToken cancellationToken)
    => await armoniKClient_.BlobService.SendDllBlobAsync(sessionInfo_,
                                                         dynamicLibrary,
                                                         zipPath,
                                                         manualDeletion,
                                                         cancellationToken)
                           .ConfigureAwait(false);

  /// <summary>
  ///   Asynchronously waits for the blobs defined with a callback, and calls their respective callbacks.
  /// </summary>
  /// <returns>A task representing the asynchronous operation.</returns>
  public async Task WaitCallbacksAsync()
  {
    if (callbackRunner_ != null)
    {
      await callbackRunner_.WaitAsync()
                           .ConfigureAwait(false);
      await callbackRunner_.DisposeAsync()
                           .ConfigureAwait(false);
      callbackRunner_ = null;
    }
  }

  /// <summary>
  ///   Aborts the execution of callbacks registered during task submission.
  /// </summary>
  /// <returns>A task representing the asynchronous operation.</returns>
  public async Task AbortCallbacksAsync()
  {
    if (callbackRunner_ != null)
    {
      await callbackRunner_.DisposeAsync()
                           .ConfigureAwait(false);
      callbackRunner_ = null;
    }
  }

  /// <summary>
  ///   Submit a collection of tasks.
  /// </summary>
  /// <param name="tasks">The tasks to submit</param>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>A task representing the asynchronous operation. The task result contains a collection of task handles.</returns>
  /// <exception cref="ArgumentException">When the tasks parameter provided is null</exception>
  public async Task<ICollection<TaskHandle>> SubmitAsync(IEnumerable<TaskDefinition> tasks,
                                                         CancellationToken           cancellationToken = default)
  {
    _ = tasks ?? throw new ArgumentException("Tasks parameter should not be null");

    var taskInfos = await armoniKClient_.TasksService.SubmitTasksAsync(sessionInfo_,
                                                                       tasks,
                                                                       cancellationToken)
                                        .ConfigureAwait(false);

    var blobsWithCallbacks = tasks.SelectMany(t => t.Outputs.Values)
                                  .Where(b => b.CallBack != null)
                                  .ToList();

    if (blobsWithCallbacks.Any())
    {
      callbackRunner_ ??= new CallbackRunner(armoniKClient_,
                                             cancellationToken);

      foreach (var blob in blobsWithCallbacks)
      {
        callbackRunner_.Add(blob);
      }
    }

    return taskInfos.Select(t => new TaskHandle(armoniKClient_,
                                                t))
                    .ToList();
  }

  private class CallbackRunner : IAsyncDisposable
  {
    private readonly ConcurrentDictionary<string, Func<BlobHandle, CancellationToken, Task>> callbacks_ = new();

    private readonly ArmoniKClient client_;

    /// <summary>
    ///   Allows to cancel the execution of the callbacks. Whenever a callback is executed and when a cancel
    ///   is requested on that token source, it requests the cancellation to that callback too.
    /// </summary>
    private readonly CancellationTokenSource cts_;

    private readonly Task workerTask_;
    private volatile bool abort_;
    private volatile bool waitAllAndQuit_;

    public CallbackRunner(ArmoniKClient     client,
                          CancellationToken cancellationToken)
    {
      client_ = client;
      cts_    = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
      workerTask_ = Task.Run(async () =>
                             {
                               await RunAsync()
                                 .ConfigureAwait(false);
                             });
    }

    /// <summary>
    ///   Abort any callback being executed and dispose the members.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
      abort_ = true;
      // Send an abort signal
      cts_.Cancel();
      // Wait for the worker task to complete
      await WaitAsync()
        .ConfigureAwait(false);
      // Dispose disposable members
      cts_.Dispose();
    }

    /// <summary>
    ///   Wait until all registered callbacks are executed.
    /// </summary>
    public async Task WaitAsync()
    {
      waitAllAndQuit_ = true;
      await workerTask_.ConfigureAwait(false);
    }

    /// <summary>
    ///   Register a new blob having a callback.
    /// </summary>
    /// <param name="blob">The blob definition carrying a callback</param>
    public void Add(BlobDefinition blob)
      => callbacks_.GetOrAdd(blob.BlobHandle!.BlobInfo.BlobId,
                             blob.CallBack!);

    /// <summary>
    ///   Execution loop
    /// </summary>
    private async Task RunAsync()
    {
      while (!cts_.Token.IsCancellationRequested)
      {
        await foreach (var blobState in client_.BlobService.GetBlobStatesByStatusAsync(callbacks_.Keys.ToList(),
                                                                                       BlobStatus.Completed,
                                                                                       cts_.Token)
                                               .ConfigureAwait(false))
        {
          if (callbacks_.TryRemove(blobState.BlobId,
                                   out var func))
          {
            await func.Invoke(new BlobHandle(blobState,
                                             client_),
                              cts_.Token)
                      .ConfigureAwait(false);
          }
          else
          {
            throw new ArmoniKSdkException($"Unexpected error: could not retrieve the callback for blob {blobState.BlobId}");
          }

          if (cts_.Token.IsCancellationRequested)
          {
            return;
          }
        }

        if (waitAllAndQuit_ && (callbacks_.IsEmpty || abort_))
        {
          return;
        }

        // Wait for 5 second and then retry
        await Task.Delay(5000,
                         cts_.Token)
                  .ConfigureAwait(false);
      }
    }
  }
}
