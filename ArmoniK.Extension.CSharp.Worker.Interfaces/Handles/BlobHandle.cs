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

namespace ArmoniK.Extension.CSharp.Worker.Interfaces.Handles;

/// <summary>
///   A handle allowing to perform some operations on a blob
/// </summary>
public class BlobHandle
{
  private readonly ISdkTaskHandler sdkTaskHandler_;

  /// <summary>
  ///   Creates an of a BlobHandle
  /// </summary>
  /// <param name="blobId">The blob id</param>
  /// <param name="sdkTaskHandler">The SDK task handler</param>
  /// <param name="data">The blob's raw data for input blobs</param>
  public BlobHandle(string          blobId,
                    ISdkTaskHandler sdkTaskHandler,
                    byte[]?         data = null)
  {
    BlobId          = blobId;
    sdkTaskHandler_ = sdkTaskHandler;
    Data            = data;
  }

  /// <summary>
  ///   The blob id
  /// </summary>
  public string BlobId { get; init; }

  /// <summary>
  ///   The blob raw data, null for output blobs
  /// </summary>
  public byte[]? Data { get; init; }

  /// <summary>
  ///   Decodes the blob's data as a string with a given encoding.
  /// </summary>
  /// <param name="encoding">Encoding used for the string, when null UTF-8 is used</param>
  /// <returns>The resulting string</returns>
  public string GetStringData(Encoding? encoding = null)
    => (encoding ?? Encoding.UTF8).GetString(Data!);

  /// <summary>
  ///   Set the blob data
  /// </summary>
  /// <param name="data">The blob's data</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method.</param>
  /// <returns>A task representing the asynchronous operation.</returns>
  public async Task SendResultAsync(byte[]            data,
                                    CancellationToken cancellationToken = default)
    => await sdkTaskHandler_.SendResultAsync(this,
                                             data,
                                             cancellationToken)
                            .ConfigureAwait(false);

  /// <summary>
  ///   Set the blob data
  /// </summary>
  /// <param name="data">The string result</param>
  /// <param name="encoding">Encoding used for the string, when null UTF-8 is used</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method.</param>
  /// <returns>A task representing the asynchronous operation.</returns>
  public async Task SendStringResultAsync(string            data,
                                          Encoding?         encoding          = null,
                                          CancellationToken cancellationToken = default)
    => await sdkTaskHandler_.SendStringResultAsync(this,
                                                   data,
                                                   encoding,
                                                   cancellationToken)
                            .ConfigureAwait(false);
}
