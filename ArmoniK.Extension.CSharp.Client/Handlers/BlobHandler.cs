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
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Extension.CSharp.Client.Common.Domain.Blob;

namespace ArmoniK.Extension.CSharp.Client.Handlers;

/// <summary>
///   Provides methods for handling operations related to blobs, such as retrieving state, downloading, and uploading
///   blob data.
/// </summary>
public record BlobHandler : BlobInfo
{
  /// <summary>
  ///   The ArmoniK client used for performing blob operations.
  /// </summary>
  public readonly ArmoniKClient ArmoniKClient;

  /// <summary>
  ///   Initializes a new instance of the <see cref="BlobHandler" /> class with specified blob information and an ArmoniK
  ///   client.
  /// </summary>
  /// <param name="blobInfo">The information about the blob.</param>
  /// <param name="armoniKClient">The ArmoniK client used for performing blob operations.</param>
  public BlobHandler(BlobInfo blobInfo, ArmoniKClient armoniKClient)
       : this(blobInfo.SessionId, blobInfo.BlobName, blobInfo.BlobId, armoniKClient) // Calls the constructor above
  {
  }


  /// <summary>
  ///   Initializes a new instance of the <see cref="BlobHandler" /> class with specified blob details and an ArmoniK
  ///   client.
  /// </summary>
  /// <param name="blobName">The name of the blob.</param>
  /// <param name="blobId">The identifier of the blob.</param>
  /// <param name="sessionId">The session identifier associated with the blob.</param>
  /// <param name="armoniKClient">The ArmoniK client used for performing blob operations.</param>
  public BlobHandler(string sessionId, string blobName, string blobId, ArmoniKClient armoniKClient)
  // Assuming BlobInfo has a constructor that takes these parameters
  {
    BlobName = blobName;
    BlobId = blobId;
    SessionId = sessionId;
    ArmoniKClient = armoniKClient;
  }

  /// <summary>
  ///   Asynchronously retrieves the state of the blob.
  /// </summary>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>A task representing the asynchronous operation. The task result contains the blob state.</returns>
  public async Task<BlobState> GetBlobStateAsync(CancellationToken cancellationToken = default)
  {
    var blobClient = await ArmoniKClient.GetBlobService();
    return await blobClient.GetBlobStateAsync(BlobInfo,
                                              cancellationToken)
                           .ConfigureAwait(false);
  }

  /// <summary>
  ///   Asynchronously downloads the data of the blob in chunks.
  /// </summary>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>An asynchronous enumerable of byte arrays representing the blob data chunks.</returns>
  public async IAsyncEnumerable<byte[]> DownloadBlobDataAsync([EnumeratorCancellation] CancellationToken cancellationToken)
  {
    var blobClient = await ArmoniKClient.GetBlobService();

    await foreach (var chunk in blobClient.DownloadBlobWithChunksAsync(this,
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
  {
    var blobClient = await ArmoniKClient.GetBlobService();

    // Upload the blob chunk
    await blobClient.UploadBlobAsync(this,
                                     blobContent,
                                     cancellationToken)
                    .ConfigureAwait(false);
  }
}
