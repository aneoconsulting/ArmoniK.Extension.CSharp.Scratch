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

using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1.Versions;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Versions;
using ArmoniK.Extension.CSharp.Client.Common.Services;
using ArmoniK.Utils;

using Grpc.Core;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Extension.CSharp.Client.Services;

internal class VersionsService : IVersionsService
{
  private readonly ObjectPool<ChannelBase> channel_;

  private readonly ILogger<VersionsService> logger_;

  public VersionsService(ObjectPool<ChannelBase> channel,
                         ILoggerFactory          loggerFactory)
  {
    channel_ = channel;
    logger_  = loggerFactory.CreateLogger<VersionsService>();
  }

  public async Task<VersionsInfo> GetVersion(CancellationToken cancellationToken)
  {
    await using var channel = await channel_.GetAsync(cancellationToken)
                                            .ConfigureAwait(false);
    var versionClient = new Versions.VersionsClient(channel);

    var listVersionsResponse = await versionClient.ListVersionsAsync(new ListVersionsRequest(),
                                                                     cancellationToken: cancellationToken)
                                                  .ConfigureAwait(false);

    return new VersionsInfo
           {
             Api  = listVersionsResponse.Api,
             Core = listVersionsResponse.Core,
           };
  }
}
