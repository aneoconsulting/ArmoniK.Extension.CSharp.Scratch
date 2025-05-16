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

using ArmoniK.Api.gRPC.V1;

namespace ArmoniK.Extension.CSharp.Client.Common.Domain.Blob;

/// <summary>
///   Represents the state of a blob.
/// </summary>
public record BlobState : BlobInfo
{
  /// <summary>
  ///   Datetime when the blob was set to status completed.
  /// </summary>
  public DateTime? CompletedAt { get; init; }

  /// <summary>
  ///   Datetime when the blob was created.
  /// </summary>
  public DateTime CreateAt { get; init; }

  /// <summary>
  ///   Current status of the blob.
  /// </summary>
  public BlobStatus Status { get; init; }
}

/// <summary>
///   Defines the various statuses that a blob can have.
/// </summary>
public enum BlobStatus
{
  /// <summary>
  ///   Blob is in an unspecified state.
  /// </summary>
  Unspecified = 0,

  /// <summary>
  ///   Blob is created and the task is created, submitted, or dispatched.
  /// </summary>
  Created = 1,

  /// <summary>
  ///   Blob is completed with a completed task.
  /// </summary>
  Completed = 2,

  /// <summary>
  ///   Blob is aborted.
  /// </summary>
  Aborted = 3,

  /// <summary>
  ///   Blob was deleted.
  /// </summary>
  Deleted = 4,

  /// <summary>
  ///   NOTFOUND is encoded as 127 to make it small while still leaving enough room for future status extensions.
  ///   See https://developers.google.com/protocol-buffers/docs/proto3#enum.
  /// </summary>
  Notfound = 127, // 0x0000007F
}

internal static class BlobStatusExt
{
  public static ResultStatus ToGrpcStatus(this BlobStatus status)
    => status switch
       {
         BlobStatus.Unspecified => ResultStatus.Unspecified,
         BlobStatus.Created     => ResultStatus.Created,
         BlobStatus.Completed   => ResultStatus.Completed,
         BlobStatus.Aborted     => ResultStatus.Aborted,
         BlobStatus.Deleted     => ResultStatus.Deleted,
         BlobStatus.Notfound    => ResultStatus.Notfound,
         _ => throw new ArgumentOutOfRangeException(nameof(status),
                                                    status,
                                                    null),
       };

  public static BlobStatus ToInternalStatus(this ResultStatus status)
    => status switch
       {
         ResultStatus.Unspecified => BlobStatus.Unspecified,
         ResultStatus.Created     => BlobStatus.Created,
         ResultStatus.Completed   => BlobStatus.Completed,
         ResultStatus.Aborted     => BlobStatus.Aborted,
         ResultStatus.Notfound    => BlobStatus.Notfound,
         ResultStatus.Deleted     => BlobStatus.Deleted,
         _ => throw new ArgumentOutOfRangeException(nameof(status),
                                                    status,
                                                    null),
       };
}
