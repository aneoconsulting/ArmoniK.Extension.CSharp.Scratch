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
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Blob;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Session;
using ArmoniK.Extension.CSharp.Client.Common.Enum;
using ArmoniK.Extension.CSharp.Common.Common.Domain.Blob;

namespace ArmoniK.Extension.CSharp.Client.Common.Services;

/// <summary>
///   Defines a service for managing blobs, including creating, downloading, and uploading blobs and their metadata.
/// </summary>
public interface IBlobService
{
  /// <summary>
  ///   Get a queryable object to filter and order BlobState instances
  /// </summary>
  /// <returns>An IQueryable instance to apply Linq methods on</returns>
  IQueryable<BlobState> AsQueryable();

  /// <summary>
  ///   Asynchronously creates metadata for multiple blobs in a given session.
  /// </summary>
  /// <param name="session">The session information in which the blobs are created.</param>
  /// <param name="names">
  ///   The names of the blobs to create metadata for and whether
  ///   the blob should be deleted manually or not
  /// </param>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>An asynchronous enumerable of blob information objects.</returns>
  IAsyncEnumerable<BlobInfo> CreateBlobsMetadataAsync(SessionInfo                                     session,
                                                      IEnumerable<(string name, bool manualDeletion)> names,
                                                      CancellationToken                               cancellationToken = default);

  /// <summary>
  ///   Asynchronously creates metadata for multiple blobs in a given session.
  /// </summary>
  /// <param name="session">The session information in which the blobs are created.</param>
  /// <param name="names">The names of the blobs to create metadata for.</param>
  /// <param name="manualDeletion">Whether the blobs should be deleted manually or not.</param>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>An asynchronous enumerable of blob information objects.</returns>
  IAsyncEnumerable<BlobInfo> CreateBlobsMetadataAsync(SessionInfo         session,
                                                      IEnumerable<string> names,
                                                      bool                manualDeletion    = false,
                                                      CancellationToken   cancellationToken = default);

  /// <summary>
  ///   Asynchronously creates a blob with the specified content in a given session.
  /// </summary>
  /// <param name="session">The session information in which the blob is created.</param>
  /// <param name="name">The name of the blob to create.</param>
  /// <param name="content">The content of the blob to create.</param>
  /// <param name="manualDeletion">Whether the blob should be deleted manually.</param>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>A task representing the asynchronous operation. The task result contains the created blob information.</returns>
  Task<BlobInfo> CreateBlobAsync(SessionInfo          session,
                                 string               name,
                                 ReadOnlyMemory<byte> content,
                                 bool                 manualDeletion    = false,
                                 CancellationToken    cancellationToken = default);

  /// <summary>
  ///   Asynchronously creates multiple blobs from blob definitions.
  ///   The blob definition's blob handles are set with the result.
  /// </summary>
  /// <param name="session">The session information in which the blobs are created.</param>
  /// <param name="blobDefinitions">The blob definitions</param>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>A task representing the asynchronous operation.</returns>
  public Task CreateBlobsAsync(SessionInfo                 session,
                               IEnumerable<BlobDefinition> blobDefinitions,
                               CancellationToken           cancellationToken = default);

  /// <summary>
  ///   Asynchronously creates multiple blobs with the specified names and contents in a given session.
  ///   Deprecated: this method is a dependency of class TaskNode, it will be removed with TaskNode.
  /// </summary>
  /// <param name="session">The session information in which the blobs are created.</param>
  /// <param name="blobContents">
  ///   The tuples representing blob names, their contents
  ///   and whether the blob should be manually deleted.
  /// </param>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>An asynchronous enumerable of blob information objects.</returns>
  [Obsolete]
  IAsyncEnumerable<BlobInfo> CreateBlobsAsync(SessionInfo                                                                   session,
                                              IEnumerable<(string name, ReadOnlyMemory<byte> content, bool manualDeletion)> blobContents,
                                              CancellationToken                                                             cancellationToken = default);

  /// <summary>
  ///   Asynchronously downloads the content of a blob.
  /// </summary>
  /// <param name="blobInfo">The information of the blob to download.</param>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>A task representing the asynchronous operation. The task result contains the blob content as a byte array.</returns>
  Task<byte[]> DownloadBlobAsync(BlobInfo          blobInfo,
                                 CancellationToken cancellationToken = default);

  /// <summary>
  ///   Asynchronously downloads the content of a blob in chunks.
  /// </summary>
  /// <param name="blobInfo">The information of the blob to download.</param>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>An asynchronous enumerable of byte arrays representing the blob content chunks.</returns>
  IAsyncEnumerable<byte[]> DownloadBlobWithChunksAsync(BlobInfo          blobInfo,
                                                       CancellationToken cancellationToken = default);

  /// <summary>
  ///   Asynchronously uploads the content chunks of a blob.
  /// </summary>
  /// <param name="blobInfo">The information of the blob to upload.</param>
  /// <param name="blobContent">The content chunks to upload.</param>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>A task representing the asynchronous operation.</returns>
  Task UploadBlobAsync(BlobInfo             blobInfo,
                       ReadOnlyMemory<byte> blobContent,
                       CancellationToken    cancellationToken = default);


  /// <summary>
  ///   Asynchronously retrieves the state of a blob.
  /// </summary>
  /// <param name="blobInfo">The information of the blob to retrieve the state for.</param>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>A task representing the asynchronous operation. The task result contains the blob state.</returns>
  Task<BlobState> GetBlobStateAsync(BlobInfo          blobInfo,
                                    CancellationToken cancellationToken = default);

  /// <summary>
  ///   Asynchronously retrieves the states of a blob collection having a specific status.
  /// </summary>
  /// <param name="blobIds">The collection blob ids to retrieve the state for.</param>
  /// <param name="status">The filtering status.</param>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns> An asynchronous enumerable of blob information instances satisfying the filter.</returns>
  IAsyncEnumerable<BlobState> GetBlobStatesByStatusAsync(IEnumerable<string> blobIds,
                                                         BlobStatus          status,
                                                         CancellationToken   cancellationToken = default);

  /// <summary>
  ///   Asynchronously lists blobs based on pagination options.
  /// </summary>
  /// <param name="blobPagination">The options for pagination, including page number, page size, and sorting.</param>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>An asynchronous enumerable of blob pages.</returns>
  Task<BlobPage> ListBlobsAsync(BlobPagination    blobPagination,
                                CancellationToken cancellationToken = default);

  /// <summary>
  ///   Import existing data from the object storage into existing results.
  /// </summary>
  /// <param name="session">The session information in which the blob is created.</param>
  /// <param name="blobDescs">The BlobInfo associated with its OpaqueId.</param>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns></returns>
  Task<ICollection<BlobState>> ImportBlobDataAsync(SessionInfo                                 session,
                                                   IEnumerable<KeyValuePair<BlobInfo, byte[]>> blobDescs,
                                                   CancellationToken                           cancellationToken = default);
}

/// <summary>
///   Provides extension methods for the <see cref="IBlobService" /> interface.
/// </summary>
public static class BlobServiceExt
{
  /// <summary>
  ///   Asynchronously creates metadata for a specified number of blobs in a given session.
  /// </summary>
  /// <param name="blobService">The blob service instance.</param>
  /// <param name="session">The session information in which the blobs are created.</param>
  /// <param name="quantity">The number of blobs to create metadata for.</param>
  /// <param name="manualDeletion">Whether the blobs should be deleted manually.</param>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>An asynchronous enumerable of blob information objects.</returns>
  public static IAsyncEnumerable<BlobInfo> CreateBlobsMetadataAsync(this IBlobService blobService,
                                                                    SessionInfo       session,
                                                                    int               quantity,
                                                                    bool              manualDeletion    = false,
                                                                    CancellationToken cancellationToken = default)
    => blobService.CreateBlobsMetadataAsync(session,
                                            Enumerable.Range(0,
                                                             quantity)
                                                      .Select(_ => (Guid.NewGuid()
                                                                        .ToString(), manualDeletion))
                                                      .ToList(),
                                            cancellationToken);

  /// <summary>
  ///   Asynchronously lists all blobs in a given session with support for pagination.
  /// </summary>
  /// <param name="blobService">The blob service instance.</param>
  /// <param name="session">The session information in which the blobs are listed.</param>
  /// <param name="pageSize">The number of blobs to retrieve per page.</param>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>An asynchronous enumerable of BlobState.</returns>
  public static async IAsyncEnumerable<BlobState> ListAllBlobsAsync(this IBlobService                          blobService,
                                                                    SessionInfo                                session,
                                                                    int                                        pageSize          = 50,
                                                                    [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    var blobPagination = new BlobPagination
                         {
                           Filter = new Filters
                                    {
                                      Or =
                                      {
                                        new FiltersAnd
                                        {
                                          And =
                                          {
                                            new FilterField
                                            {
                                              Field = new ResultField
                                                      {
                                                        ResultRawField = new ResultRawField
                                                                         {
                                                                           Field = ResultRawEnumField.SessionId,
                                                                         },
                                                      },
                                              FilterString = new FilterString
                                                             {
                                                               Operator = FilterStringOperator.Equal,
                                                               Value    = session.SessionId,
                                                             },
                                            },
                                          },
                                        },
                                      },
                                    },
                           Page          = 0,
                           PageSize      = pageSize,
                           SortDirection = SortDirection.Asc,
                           SortField = new ResultField
                                       {
                                         ResultRawField = new ResultRawField
                                                          {
                                                            Field = ResultRawEnumField.ResultId,
                                                          },
                                       },
                         };

    var total = 0;

    BlobPage page;
    do
    {
      page = await blobService.ListBlobsAsync(blobPagination,
                                              cancellationToken)
                              .ConfigureAwait(false);
      total += page.Blobs.Length;
      foreach (var blobState in page.Blobs)
      {
        yield return blobState;
      }

      blobPagination.Page++;
    } while (total < page.TotalBlobCount);
  }
}
