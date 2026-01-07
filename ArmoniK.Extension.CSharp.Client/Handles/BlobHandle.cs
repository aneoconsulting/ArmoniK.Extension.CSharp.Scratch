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

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Extension.CSharp.Common.Common.Domain.Blob;

namespace ArmoniK.Extension.CSharp.Client.Handles;

/// <summary>
///   Provides methods for handling operations related to blobs, such as retrieving state, downloading, and uploading
///   blob data.
/// </summary>
public class BlobHandle
{
  /// <summary>
  ///   The ArmoniK client used for performing blob operations.
  /// </summary>
  public readonly ArmoniKClient ArmoniKClient;

  /// <summary>
  ///   The blob containing name, session ID, and blob ID.
  /// </summary>
  public readonly BlobInfo BlobInfo;

  /// <summary>
  ///   Initializes a new instance of the <see cref="BlobHandle" /> class with specified blob information and an ArmoniK
  ///   client.
  /// </summary>
  /// <param name="blobInfo">The information about the blob.</param>
  /// <param name="armoniKClient">The ArmoniK client used for performing blob operations.</param>
  public BlobHandle(BlobInfo      blobInfo,
                    ArmoniKClient armoniKClient)

  {
    BlobInfo      = blobInfo      ?? throw new ArgumentNullException(nameof(blobInfo));
    ArmoniKClient = armoniKClient ?? throw new ArgumentNullException(nameof(armoniKClient));
  }

  /// <summary>
  ///   Initializes a new instance of the <see cref="BlobHandle" /> class with specified blob details and an ArmoniK
  ///   client.
  /// </summary>
  /// <param name="blobName">The name of the blob.</param>
  /// <param name="blobId">The identifier of the blob.</param>
  /// <param name="sessionId">The session identifier associated with the blob.</param>
  /// <param name="armoniKClient">The ArmoniK client used for performing blob operations.</param>
  public BlobHandle(string        blobName,
                    string        blobId,
                    string        sessionId,
                    ArmoniKClient armoniKClient)
  {
    BlobInfo = new BlobInfo
               {
                 BlobId    = blobId,
                 BlobName  = blobName,
                 SessionId = sessionId,
               };
    ArmoniKClient = armoniKClient ?? throw new ArgumentNullException(nameof(armoniKClient));
  }

  /// <summary>
  ///   Implicit conversion operator from BlobHandle to BlobInfo.
  ///   Allows BlobHandle to be used wherever BlobInfo is expected.
  /// </summary>
  /// <param name="blobHandle">The BlobHandle to convert.</param>
  /// <returns>The BlobInfo contained within the BlobHandle.</returns>
  /// <exception cref="ArgumentNullException">Thrown when blobHandle is null.</exception>
  public static implicit operator BlobInfo(BlobHandle blobHandle)
    => blobHandle?.BlobInfo ?? throw new ArgumentNullException(nameof(blobHandle));


  /// <summary>
  ///   Creates a BlobHandle from BlobInfo and ArmoniKClient.
  /// </summary>
  /// <param name="blobInfo">The BlobInfo to wrap.</param>
  /// <param name="armoniKClient">The ArmoniK client for operations.</param>
  /// <returns>A new BlobHandle instance.</returns>
  /// <exception cref="ArgumentNullException">Thrown when blobInfo or armoniKClient is null.</exception>
  public static BlobHandle FromBlobInfo(BlobInfo      blobInfo,
                                        ArmoniKClient armoniKClient)
    => new(blobInfo      ?? throw new ArgumentNullException(nameof(blobInfo)),
           armoniKClient ?? throw new ArgumentNullException(nameof(armoniKClient)));

  /// <summary>
  ///   Asynchronously retrieves the state of the blob.
  /// </summary>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>A task representing the asynchronous operation. The task result contains the blob state.</returns>
  public async Task<BlobState> GetBlobStateAsync(CancellationToken cancellationToken = default)
    => await ArmoniKClient.BlobService.GetBlobStateAsync(this,
                                                         cancellationToken)
                          .ConfigureAwait(false);

  /// <summary>
  ///   Asynchronously downloads the data of the blob.
  /// </summary>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>A task representing the asynchronous operation. The task result contains the blob content as a byte array.</returns>
  public async Task<byte[]> DownloadBlobDataAsync(CancellationToken cancellationToken)
    => await ArmoniKClient.BlobService.DownloadBlobAsync(this,
                                                         cancellationToken)
                          .ConfigureAwait(false);

  /// <summary>
  ///   Asynchronously downloads the data of the blob in chunks.
  /// </summary>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>An asynchronous enumerable of byte arrays representing the blob data chunks.</returns>
  public async IAsyncEnumerable<byte[]> DownloadBlobDataWithChunksAsync([EnumeratorCancellation] CancellationToken cancellationToken)
  {
    await foreach (var chunk in ArmoniKClient.BlobService.DownloadBlobWithChunksAsync(this,
                                                                                      cancellationToken)
                                             .ConfigureAwait(false))
    {
      yield return chunk;
    }
  }

  /// <summary>
  ///   Asynchronously uploads the specified content to the blob.
  /// </summary>
  /// <param name="blobContent">The content to upload to the blob.</param>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>A task representing the asynchronous operation.</returns>
  public async Task UploadBlobDataAsync(ReadOnlyMemory<byte> blobContent,
                                        CancellationToken    cancellationToken)
    // Upload the blob chunk
    => await ArmoniKClient.BlobService.UploadBlobAsync(this,
                                                       blobContent,
                                                       cancellationToken)
                          .ConfigureAwait(false);
}
