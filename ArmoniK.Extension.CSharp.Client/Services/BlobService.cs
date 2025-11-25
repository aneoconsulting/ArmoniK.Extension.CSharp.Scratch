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

using ArmoniK.Api.Client;
using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Blob;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Session;
using ArmoniK.Extension.CSharp.Client.Common.Enum;
using ArmoniK.Extension.CSharp.Client.Common.Services;
using ArmoniK.Extension.CSharp.Client.Exceptions;
using ArmoniK.Extension.CSharp.Client.Handles;
using ArmoniK.Extension.CSharp.Client.Queryable;
using ArmoniK.Utils;

using Google.Protobuf;

using Grpc.Core;

using Microsoft.Extensions.Logging;

using static ArmoniK.Api.gRPC.V1.Results.ImportResultsDataRequest.Types;

namespace ArmoniK.Extension.CSharp.Client.Services;

/// <inheritdoc />
public class BlobService : IBlobService
{
  private readonly ArmoniKClient                        armoniKClient_;
  private readonly ObjectPool<ChannelBase>              channelPool_;
  private readonly ILogger<BlobService>                 logger_;
  private readonly ArmoniKQueryable<BlobState>          queryable_;
  private          ResultsServiceConfigurationResponse? serviceConfiguration_;

  /// <summary>
  ///   Creates an instance of <see cref="BlobService" /> using the specified GRPC channel and an optional logger factory.
  /// </summary>
  /// <param name="channel">
  ///   An object pool that manages GRPC channels, providing efficient handling and reuse of channel
  ///   resources.
  /// </param>
  /// <param name="armoniKClient">The ArmoniK client.</param>
  /// <param name="loggerFactory">
  ///   An optional factory for creating loggers, which can be used to enable logging within the
  ///   blob service. If null, logging will be disabled.
  /// </param>
  public BlobService(ObjectPool<ChannelBase> channel,
                     ArmoniKClient           armoniKClient,
                     ILoggerFactory          loggerFactory)
  {
    channelPool_   = channel;
    armoniKClient_ = armoniKClient;
    logger_        = loggerFactory.CreateLogger<BlobService>();

    var queryProvider = new BlobStateQueryProvider(this,
                                                   logger_);
    queryable_ = new ArmoniKQueryable<BlobState>(queryProvider);
  }

  /// <inheritdoc />
  public IQueryable<BlobState> AsQueryable()
    => queryable_;


  /// <inheritdoc />
  public async IAsyncEnumerable<BlobInfo> CreateBlobsMetadataAsync(SessionInfo                                     session,
                                                                   IEnumerable<(string name, bool manualDeletion)> names,
                                                                   [EnumeratorCancellation] CancellationToken      cancellationToken = default)
  {
    await using var channel = await channelPool_.GetAsync(cancellationToken)
                                                .ConfigureAwait(false);
    var blobClient = new Results.ResultsClient(channel);

    var resultsCreate = names.Select(blob => new CreateResultsMetaDataRequest.Types.ResultCreate
                                             {
                                               Name           = blob.name,
                                               ManualDeletion = blob.manualDeletion,
                                             });

    var blobsCreationResponse = await blobClient.CreateResultsMetaDataAsync(new CreateResultsMetaDataRequest
                                                                            {
                                                                              SessionId = session.SessionId,
                                                                              Results =
                                                                              {
                                                                                resultsCreate,
                                                                              },
                                                                            },
                                                                            cancellationToken: cancellationToken)
                                                .ConfigureAwait(false);

    var asyncBlobInfos = blobsCreationResponse.Results.Select(b => new BlobInfo
                                                                   {
                                                                     BlobName  = b.Name,
                                                                     BlobId    = b.ResultId,
                                                                     SessionId = session.SessionId,
                                                                   })
                                              .ToAsyncEnumerable();

    await foreach (var blobInfo in asyncBlobInfos.WithCancellation(cancellationToken)
                                                 .ConfigureAwait(false))
    {
      yield return blobInfo;
    }
  }

  /// <inheritdoc />
  public async Task<byte[]> DownloadBlobAsync(BlobInfo          blobInfo,
                                              CancellationToken cancellationToken = default)
  {
    try
    {
      await using var channel = await channelPool_.GetAsync(cancellationToken)
                                                  .ConfigureAwait(false);
      var blobClient = new Results.ResultsClient(channel);
      return await blobClient.DownloadResultData(blobInfo.SessionId,
                                                 blobInfo.BlobId,
                                                 cancellationToken)
                             .ConfigureAwait(false);
    }
    catch (Exception e)
    {
      logger_.LogError(e.Message);
      throw;
    }
  }

  /// <inheritdoc />
  public async IAsyncEnumerable<byte[]> DownloadBlobWithChunksAsync(BlobInfo                                   blobInfo,
                                                                    [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    await using var channel = await channelPool_.GetAsync(cancellationToken)
                                                .ConfigureAwait(false);
    var blobClient = new Results.ResultsClient(channel);
    var stream = blobClient.DownloadResultData(new DownloadResultDataRequest
                                               {
                                                 ResultId  = blobInfo.BlobId,
                                                 SessionId = blobInfo.SessionId,
                                               },
                                               cancellationToken: cancellationToken);
    while (await stream.ResponseStream.MoveNext(cancellationToken)
                       .ConfigureAwait(false))
    {
      yield return stream.ResponseStream.Current.DataChunk.ToByteArray();
    }
  }

  /// <inheritdoc />
  public async Task UploadBlobAsync(BlobInfo             blobInfo,
                                    ReadOnlyMemory<byte> blobContent,
                                    CancellationToken    cancellationToken = default)
  {
    await using var channel = await channelPool_.GetAsync(cancellationToken)
                                                .ConfigureAwait(false);
    var blobClient = new Results.ResultsClient(channel);

    await UploadBlobAsync(blobInfo,
                          blobContent,
                          blobClient,
                          cancellationToken)
      .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async Task<BlobState> GetBlobStateAsync(BlobInfo          blobInfo,
                                                 CancellationToken cancellationToken = default)
  {
    await using var channel = await channelPool_.GetAsync(cancellationToken)
                                                .ConfigureAwait(false);
    var blobClient = new Results.ResultsClient(channel);
    var blobDetails = await blobClient.GetResultAsync(new GetResultRequest
                                                      {
                                                        ResultId = blobInfo.BlobId,
                                                      })
                                      .ConfigureAwait(false);
    return blobDetails.Result.ToBlobState();
  }

  /// <inheritdoc />
  public async Task<BlobInfo> CreateBlobAsync(SessionInfo          session,
                                              string               name,
                                              ReadOnlyMemory<byte> content,
                                              bool                 manualDeletion    = false,
                                              CancellationToken    cancellationToken = default)
  {
    if (serviceConfiguration_ is null)
    {
      await LoadBlobServiceConfigurationAsync(cancellationToken)
        .ConfigureAwait(false);
    }

    await using var channel = await channelPool_.GetAsync(cancellationToken)
                                                .ConfigureAwait(false);
    var blobClient = new Results.ResultsClient(channel);

    if (serviceConfiguration_ != null && content.Length > serviceConfiguration_.DataChunkMaxSize)
    {
      var blobInfo = CreateBlobsMetadataAsync(session,
                                              [(name, manualDeletion)],
                                              cancellationToken);
      var createdBlobs = await blobInfo.ToListAsync(cancellationToken)
                                       .ConfigureAwait(false);
      await UploadBlobAsync(createdBlobs.First(),
                            content,
                            blobClient,
                            cancellationToken)
        .ConfigureAwait(false);
      return createdBlobs.First();
    }

    var blobCreationResponse = await blobClient.CreateResultsAsync(new CreateResultsRequest
                                                                   {
                                                                     SessionId = session.SessionId,
                                                                     Results =
                                                                     {
                                                                       new CreateResultsRequest.Types.ResultCreate
                                                                       {
                                                                         Name           = name,
                                                                         Data           = ByteString.CopyFrom(content.Span),
                                                                         ManualDeletion = manualDeletion,
                                                                       },
                                                                     },
                                                                   },
                                                                   cancellationToken: cancellationToken)
                                               .ConfigureAwait(false);

    return new BlobInfo
           {
             BlobName = name,
             BlobId = blobCreationResponse.Results.Single()
                                          .ResultId,
             SessionId = session.SessionId,
           };
  }

  /// <inheritdoc />
  public async Task<BlobPage> ListBlobsAsync(BlobPagination    blobPagination,
                                             CancellationToken cancellationToken = default)
  {
    await using var channel = await channelPool_.GetAsync(cancellationToken)
                                                .ConfigureAwait(false);
    var blobClient = new Results.ResultsClient(channel);
    var listResultsResponse = await blobClient.ListResultsAsync(new ListResultsRequest
                                                                {
                                                                  Sort = new ListResultsRequest.Types.Sort
                                                                         {
                                                                           Direction = blobPagination.SortDirection.ToGrpc(),
                                                                           Field     = blobPagination.SortField,
                                                                         },
                                                                  Filters  = blobPagination.Filter,
                                                                  Page     = blobPagination.Page,
                                                                  PageSize = blobPagination.PageSize,
                                                                },
                                                                cancellationToken: cancellationToken)
                                              .ConfigureAwait(false);

    return new BlobPage
           {
             TotalBlobCount = listResultsResponse.Total,
             PageOrder      = blobPagination.Page,
             Blobs = listResultsResponse.Results.Select(result => result.ToBlobState())
                                        .ToArray(),
           };
  }

  /// <inheritdoc />
  public async Task<ICollection<BlobState>> ImportBlobDataAsync(SessionInfo                                 session,
                                                                IEnumerable<KeyValuePair<BlobInfo, byte[]>> blobDescs,
                                                                CancellationToken                           cancellationToken = default)
  {
    await using var channel = await channelPool_.GetAsync(cancellationToken)
                                                .ConfigureAwait(false);
    var blobClient = new Results.ResultsClient(channel);
    var request = new ImportResultsDataRequest
                  {
                    SessionId = session.SessionId,
                  };
    foreach (var blobDesc in blobDescs)
    {
      request.Results.Add(new ResultOpaqueId
                          {
                            ResultId = blobDesc.Key.BlobId,
                            OpaqueId = ByteString.CopyFrom(blobDesc.Value),
                          });
    }

    var response = await blobClient.ImportResultsDataAsync(request)
                                   .ConfigureAwait(false);
    return response.Results.Select(resultRaw => resultRaw.ToBlobState())
                   .AsICollection();
  }

  /// <inheritdoc />
  public IAsyncEnumerable<BlobInfo> CreateBlobsMetadataAsync(SessionInfo         session,
                                                             IEnumerable<string> names,
                                                             bool                manualDeletion    = false,
                                                             CancellationToken   cancellationToken = default)
    => CreateBlobsMetadataAsync(session,
                                names.Select(n => (n, manualDeletion)),
                                cancellationToken);

  /// <inheritdoc />
  public async IAsyncEnumerable<BlobInfo> CreateBlobsAsync(SessionInfo                                                                   session,
                                                           IEnumerable<(string name, ReadOnlyMemory<byte> content, bool manualDeletion)> blobContents,
                                                           [EnumeratorCancellation] CancellationToken                                    cancellationToken = default)
  {
    var tasks = blobContents.Select(blob => Task.Run(async () =>
                                                     {
                                                       var blobInfo = await CreateBlobAsync(session,
                                                                                            blob.name,
                                                                                            blob.content,
                                                                                            blob.manualDeletion,
                                                                                            cancellationToken)
                                                                        .ConfigureAwait(false);
                                                       return blobInfo;
                                                     },
                                                     cancellationToken))
                            .ToList();

    var blobCreationResponse = await Task.WhenAll(tasks)
                                         .ConfigureAwait(false);

    foreach (var blob in blobCreationResponse)
    {
      yield return new BlobInfo
                   {
                     BlobName  = blob.BlobName,
                     BlobId    = blob.BlobId,
                     SessionId = session.SessionId,
                   };
    }
  }

  /// <inheritdoc />
  public async IAsyncEnumerable<BlobState> GetBlobStatesByStatusAsync(IEnumerable<string>                        blobIds,
                                                                      BlobStatus                                 status,
                                                                      [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    var chunkSize     = 1000;
    var blobIdsChunks = blobIds.ToChunks(chunkSize);

    foreach (var chunk in blobIdsChunks)
    {
      var blobPagination = new BlobPagination
                           {
                             Filter = new Filters
                                      {
                                        Or =
                                        {
                                          chunk.Select(b => new FiltersAnd
                                                            {
                                                              And =
                                                              {
                                                                new FilterField
                                                                {
                                                                  Field = new ResultField
                                                                          {
                                                                            ResultRawField = new ResultRawField
                                                                                             {
                                                                                               Field = ResultRawEnumField.ResultId,
                                                                                             },
                                                                          },
                                                                  FilterString = new FilterString
                                                                                 {
                                                                                   Operator = FilterStringOperator.Equal,
                                                                                   Value    = b,
                                                                                 },
                                                                },
                                                                new FilterField
                                                                {
                                                                  Field = new ResultField
                                                                          {
                                                                            ResultRawField = new ResultRawField
                                                                                             {
                                                                                               Field = ResultRawEnumField.Status,
                                                                                             },
                                                                          },
                                                                  FilterStatus = new FilterStatus
                                                                                 {
                                                                                   Operator = FilterStatusOperator.Equal,
                                                                                   Value    = status.ToGrpcStatus(),
                                                                                 },
                                                                },
                                                              },
                                                            }),
                                        },
                                      },
                             Page          = 0,
                             PageSize      = chunkSize,
                             SortDirection = SortDirection.Asc,
                             SortField = new ResultField
                                         {
                                           ResultRawField = new ResultRawField
                                                            {
                                                              Field = ResultRawEnumField.ResultId,
                                                            },
                                         },
                           };
      var page = await ListBlobsAsync(blobPagination,
                                      cancellationToken)
                   .ConfigureAwait(false);
      foreach (var blobState in page.Blobs)
      {
        yield return blobState;
      }
    }
  }

  private async Task LoadBlobServiceConfigurationAsync(CancellationToken cancellationToken = default)
  {
    await using var channel = await channelPool_.GetAsync(cancellationToken)
                                                .ConfigureAwait(false);
    var blobClient = new Results.ResultsClient(channel);
    serviceConfiguration_ = await blobClient.GetServiceConfigurationAsync(new Empty())
                                            .ConfigureAwait(false);
  }

  /// <summary>
  ///   Upload a blob by chunks
  /// </summary>
  /// <param name="blobDefinition">The blob definition</param>
  /// <param name="chunkEnumerator">A chunk enumerator positioned on the first chunk, or null</param>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>A task representing the asynchronous operation. The task result contains the result of the upload.</returns>
  private async Task<UploadResultDataResponse> UploadBlobByChunkAsync(BlobDefinition                          blobDefinition,
                                                                      IAsyncEnumerator<ReadOnlyMemory<byte>>? chunkEnumerator   = null,
                                                                      CancellationToken                       cancellationToken = default)
  {
    await using var channel = await channelPool_.GetAsync(cancellationToken)
                                                .ConfigureAwait(false);
    var blobClient = new Results.ResultsClient(channel);

    var stream = blobClient.UploadResultData(cancellationToken: cancellationToken);
    await stream.RequestStream.WriteAsync(new UploadResultDataRequest
                                          {
                                            Id = new UploadResultDataRequest.Types.ResultIdentifier
                                                 {
                                                   ResultId  = blobDefinition.BlobHandle!.BlobInfo.BlobId,
                                                   SessionId = blobDefinition.BlobHandle!.BlobInfo.SessionId,
                                                 },
                                          })
                .ConfigureAwait(false);

    if (chunkEnumerator == null)
    {
      chunkEnumerator = blobDefinition.GetDataAsync(serviceConfiguration_!.DataChunkMaxSize,
                                                    cancellationToken)
                                      .GetAsyncEnumerator(cancellationToken);
      // Let's move next to be positioned on the first element
      await chunkEnumerator.MoveNextAsync(cancellationToken)
                           .ConfigureAwait(false);
    }

    do
    {
      await stream.RequestStream.WriteAsync(new UploadResultDataRequest
                                            {
                                              DataChunk = UnsafeByteOperations.UnsafeWrap(chunkEnumerator.Current),
                                            })
                  .ConfigureAwait(false);
    } while (await chunkEnumerator.MoveNextAsync(cancellationToken)
                                  .ConfigureAwait(false));

    return await stream.ResponseAsync.ConfigureAwait(false);
  }


  private async Task UploadBlobAsync(BlobInfo              blob,
                                     ReadOnlyMemory<byte>  blobContent,
                                     Results.ResultsClient blobClient,
                                     CancellationToken     cancellationToken)
  {
    try
    {
      await blobClient.UploadResultData(blob.SessionId,
                                        blob.BlobId,
                                        blobContent.ToArray())
                      .ConfigureAwait(false);
    }
    catch (Exception e)
    {
      logger_.LogError(e.Message);
      throw;
    }
  }

  #region method CreateBlobsAsync() and its private dependencies

  /// <inheritdoc />
  public async Task CreateBlobsAsync(SessionInfo                 session,
                                     IEnumerable<BlobDefinition> blobDefinitions,
                                     CancellationToken           cancellationToken = default)
  {
    var blobsWithData    = new List<BlobDefinition>();
    var blobsWithoutData = new List<BlobDefinition>();

    foreach (var blobDefinition in blobDefinitions)
    {
      if (blobDefinition.BlobHandle != null)
      {
        if (blobDefinition.BlobHandle.BlobInfo.SessionId == session.SessionId)
        {
          // The blob was already created on this session, then we skip it
          continue;
        }

        if (!blobDefinition.HasData)
        {
          // The blob was created on another session, and we do not have its data.
          throw new
            ArmoniKSdkException($"The blob '{blobDefinition.BlobHandle.BlobInfo.BlobName}' (BlobId:{blobDefinition.BlobHandle.BlobInfo.BlobId}) was created on session '{blobDefinition.BlobHandle.BlobInfo.SessionId}' and cannot be used on session '{session.SessionId}'");
        }
      }

      if (blobDefinition.HasData)
      {
        if (serviceConfiguration_ is null)
        {
          await LoadBlobServiceConfigurationAsync(cancellationToken)
            .ConfigureAwait(false);
        }

        // Refresh the size information (whenever the blob is actually a file)
        blobDefinition.RefreshFile();

        if (blobDefinition.MayBeAPipe)
        {
          // The blob might come from a pipe
          var chunkEnumerator = blobDefinition.GetDataAsync(serviceConfiguration_!.DataChunkMaxSize,
                                                            cancellationToken)
                                              .GetAsyncEnumerator(cancellationToken);
          await chunkEnumerator.MoveNextAsync(cancellationToken)
                               .ConfigureAwait(false);
          if (!chunkEnumerator.Current.IsEmpty)
          {
            // This is a pipe, let's create the blob ...
            if (chunkEnumerator.Current.Length == serviceConfiguration_!.DataChunkMaxSize)
            {
              // ... by uploading it by chunks
              await UploadBlobByChunkAsync(blobDefinition,
                                           chunkEnumerator,
                                           cancellationToken)
                .ConfigureAwait(false);
            }
            else
            {
              // ... with a unitary call
              await CreateBlobAsync(session,
                                    blobDefinition.Name,
                                    chunkEnumerator.Current,
                                    blobDefinition.ManualDeletion,
                                    cancellationToken)
                .ConfigureAwait(false);
            }

            continue;
          }

          // This is an empty file
          blobDefinition.SetData(chunkEnumerator.Current);
        }

        // The blob is not a file, or it is a file but not a pipe.
        blobsWithData.Add(blobDefinition);
        if (blobDefinition.TotalSize >= serviceConfiguration_!.DataChunkMaxSize)
        {
          // For a blob size above the threshold of DataChunkMaxSize, we add it also to the list of blobs without data
          // so that its metadata will be created by CreateBlobsMetadataAsync(). Subsequently, the upload will be processed by CreateBlobsWithContentAsync()
          blobsWithoutData.Add(blobDefinition);
        }
      }
      else
      {
        blobsWithoutData.Add(blobDefinition);
      }
    }

    if (blobsWithoutData.Any())
    {
      // Creation of blobs without data
      foreach (var blobsWithoutDuplicateName in DeDuplicateWithName(blobsWithoutData))
      {
        var name2Blob = blobsWithoutDuplicateName.ToDictionary(b => b.Name,
                                                               b => b);
        var blobsCreate = blobsWithoutDuplicateName.Select(b => (b.Name, b.ManualDeletion));
        var response = CreateBlobsMetadataAsync(session,
                                                blobsCreate,
                                                cancellationToken);
        await foreach (var blob in response.ConfigureAwait(false))
        {
          name2Blob[blob.BlobName].BlobHandle = new BlobHandle(blob,
                                                               armoniKClient_);
        }
      }
    }

    if (blobsWithData.Any())
    {
      // Creation of blobs with data
      foreach (var blobsWithoutDuplicateName in DeDuplicateWithName(blobsWithData))
      {
        var name2Blob = blobsWithoutDuplicateName.ToDictionary(b => b.Name,
                                                               b => b);
        var response = CreateBlobsWithContentAsync(session,
                                                   blobsWithoutDuplicateName,
                                                   cancellationToken);
        await foreach (var blob in response.ConfigureAwait(false))
        {
          name2Blob[blob.BlobName].BlobHandle = new BlobHandle(blob,
                                                               armoniKClient_);
        }
      }
    }
  }

  private static List<List<BlobDefinition>> DeDuplicateWithName(List<BlobDefinition> blobsWithData)
  {
    var grouped = blobsWithData.GroupBy(x => x.Name)
                               .Select(g => g.ToList())
                               .ToList();
    var result = new List<List<BlobDefinition>>();

    do
    {
      var currentList = new List<BlobDefinition>();
      foreach (var list in grouped)
      {
        var blob = list.LastOrDefault();
        if (blob != null)
        {
          list.RemoveAt(list.Count - 1);
          currentList.Add(blob);
        }
      }

      if (!currentList.Any())
      {
        break;
      }

      result.Add(currentList);
    } while (true);

    return result;
  }

  private async IAsyncEnumerable<BlobInfo> CreateBlobsWithContentAsync(SessionInfo                                session,
                                                                       ICollection<BlobDefinition>                blobDefinitions,
                                                                       [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    // This is a bin packing kind of problem for which we apply the first-fit-decreasing strategy
    var batches = GetOptimizedBatches(blobDefinitions,
                                      serviceConfiguration_.DataChunkMaxSize);

    await using var channel = await channelPool_.GetAsync(cancellationToken)
                                                .ConfigureAwait(false);
    var blobClient = new Results.ResultsClient(channel);

    foreach (var batch in batches)
    {
      if (serviceConfiguration_ != null && batch.Size > serviceConfiguration_.DataChunkMaxSize)
      {
        // We are in a case where the size is above the threshold, then the batch contains a single element
        var blobDefinition = batch.Items.Single();
        await UploadBlobByChunkAsync(blobDefinition,
                                     cancellationToken: cancellationToken)
          .ConfigureAwait(false);
        yield return blobDefinition.BlobHandle!;
      }
      else
      {
        var itemTasks = batch.Items.Select(async b =>
                                           {
                                             var bytes = await b.GetDataAsync(cancellationToken: cancellationToken)
                                                                .SingleAsync(cancellationToken)
                                                                .ConfigureAwait(false);

                                             return new CreateResultsRequest.Types.ResultCreate
                                                    {
                                                      Name           = b.Name,
                                                      Data           = ByteString.CopyFrom(bytes.Span),
                                                      ManualDeletion = b.ManualDeletion,
                                                    };
                                           });
        var items = await Task.WhenAll(itemTasks)
                              .ConfigureAwait(false);

        var blobCreationResponse = await blobClient.CreateResultsAsync(new CreateResultsRequest

                                                                       {
                                                                         SessionId = session.SessionId,
                                                                         Results =
                                                                         {
                                                                           items,
                                                                         },
                                                                       },
                                                                       cancellationToken: cancellationToken)
                                                   .ConfigureAwait(false);
        foreach (var blob in blobCreationResponse.Results)
        {
          yield return blob.ToBlobState();
        }
      }
    }
  }

  /// <summary>
  ///   Dispatches a list of blob definitions in a minimal number of batches, each batch size being less than the 'maxSize'
  /// </summary>
  /// <param name="blobDefinitions">The list of blob definitions to dispatch</param>
  /// <param name="maxSize">The maximum size for a batch</param>
  /// <returns>The list of batches created</returns>
  private static List<Batch> GetOptimizedBatches(IEnumerable<BlobDefinition> blobDefinitions,
                                                 int                         maxSize)
  {
    var blobsByDescendingSize = blobDefinitions.OrderByDescending(b => b.TotalSize)
                                               .ToList();
    var batches = new List<Batch>();
    foreach (var blob in blobsByDescendingSize)
    {
      var batch = batches.FirstOrDefault(b => maxSize > b.Size + blob.TotalSize);
      if (batch == null)
      {
        batch = new Batch();
        batches.Add(batch);
      }

      batch.AddItem(blob);
    }

    return batches;
  }

  private class Batch
  {
    public List<BlobDefinition> Items { get; } = new();

    public long Size { get; private set; }

    public void AddItem(BlobDefinition item)
    {
      Items.Add(item);
      Size += item.TotalSize;
    }
  }

  #endregion
}
