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
using ArmoniK.Extension.CSharp.Client.Queryable;
using ArmoniK.Utils;

using Google.Protobuf;

using Grpc.Core;

using Microsoft.Extensions.Logging;

using static ArmoniK.Api.gRPC.V1.Results.ImportResultsDataRequest.Types;

namespace ArmoniK.Extension.CSharp.Client.Services;

public class BlobService : IBlobService
{
  private readonly ObjectPool<ChannelBase>             channelPool_;
  private readonly ILogger<BlobService>                logger_;
  private readonly ArmoniKQueryable<BlobState>         queryable_;
  private          ResultsServiceConfigurationResponse serviceConfiguration_;

  /// <summary>
  ///   Creates an instance of <see cref="BlobService" /> using the specified GRPC channel and an optional logger factory.
  /// </summary>
  /// <param name="channel">
  ///   An object pool that manages GRPC channels, providing efficient handling and reuse of channel
  ///   resources.
  /// </param>
  /// <param name="loggerFactory">
  ///   An optional factory for creating loggers, which can be used to enable logging within the
  ///   blob service. If null, logging will be disabled.
  /// </param>
  public BlobService(ObjectPool<ChannelBase> channel,
                     ILoggerFactory          loggerFactory)
  {
    channelPool_ = channel;
    logger_      = loggerFactory.CreateLogger<BlobService>();

    var queryProvider = new BlobStateQueryProvider(this,
                                                   logger_);
    queryable_ = new ArmoniKQueryable<BlobState>(queryProvider);
  }

  public IQueryable<BlobState> AsQueryable()
    => queryable_;

  public async IAsyncEnumerable<BlobInfo> CreateBlobsMetadataAsync(SessionInfo                                session,
                                                                   IEnumerable<string>                        names,
                                                                   bool                                       manualDeletion    = false,
                                                                   [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    await using var channel = await channelPool_.GetAsync(cancellationToken)
                                                .ConfigureAwait(false);
    var blobClient = new Results.ResultsClient(channel);

    var resultsCreate = names.Select(blobName => new CreateResultsMetaDataRequest.Types.ResultCreate
                                                 {
                                                   Name           = blobName,
                                                   ManualDeletion = manualDeletion,
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
                                              new[]
                                              {
                                                name,
                                              },
                                              manualDeletion,
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

  public async IAsyncEnumerable<BlobInfo> CreateBlobsAsync(SessionInfo                                             session,
                                                           IEnumerable<KeyValuePair<string, ReadOnlyMemory<byte>>> blobKeyValuePairs,
                                                           bool                                                    manualDeletion    = false,
                                                           [EnumeratorCancellation] CancellationToken              cancellationToken = default)
  {
    var tasks = blobKeyValuePairs.Select(blobKeyValuePair => Task.Run(async () =>
                                                                      {
                                                                        var blobInfo = await CreateBlobAsync(session,
                                                                                                             blobKeyValuePair.Key,
                                                                                                             blobKeyValuePair.Value,
                                                                                                             manualDeletion,
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

  private async Task LoadBlobServiceConfigurationAsync(CancellationToken cancellationToken = default)
  {
    await using var channel = await channelPool_.GetAsync(cancellationToken)
                                                .ConfigureAwait(false);
    var blobClient = new Results.ResultsClient(channel);
    serviceConfiguration_ = await blobClient.GetServiceConfigurationAsync(new Empty())
                                            .ConfigureAwait(false);
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
}
