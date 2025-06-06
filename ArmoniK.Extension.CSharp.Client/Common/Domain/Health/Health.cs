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

using Google.Protobuf.Reflection;

namespace ArmoniK.Extension.CSharp.Client.Common.Domain.Health;

/// <summary>
///   Represents the health status of a component.
/// </summary>
public record Health
{
  /// <summary>
  ///   Name of the component.
  /// </summary>
  public string Name { get; init; }

  /// <summary>
  ///   Message providing additional details about the health status.
  /// </summary>
  public string Message { get; init; }

  /// <summary>
  ///   Current health status of the component.
  /// </summary>
  public HealthStatusEnum Status { get; init; }
}

/// <summary>
///   Defines the health status values for a component.
/// </summary>
public enum HealthStatusEnum
{
  /// <summary>
  ///   The health status is unspecified.
  /// </summary>
  [OriginalName("HEALTH_STATUS_ENUM_UNSPECIFIED")]
  Unspecified,

  /// <summary>
  ///   The service is working without issues.
  /// </summary>
  [OriginalName("HEALTH_STATUS_ENUM_HEALTHY")]
  Healthy,

  /// <summary>
  ///   The service has issues but still works.
  /// </summary>
  [OriginalName("HEALTH_STATUS_ENUM_DEGRADED")]
  Degraded,

  /// <summary>
  ///   The service does not work.
  /// </summary>
  [OriginalName("HEALTH_STATUS_ENUM_UNHEALTHY")]
  Unhealthy,
}

public static class HealthStatusExt
{
  public static Api.gRPC.V1.HealthChecks.HealthStatusEnum ToGrpcStatus(this HealthStatusEnum status)
    => status switch
       {
         HealthStatusEnum.Unspecified => Api.gRPC.V1.HealthChecks.HealthStatusEnum.Unspecified,
         HealthStatusEnum.Healthy     => Api.gRPC.V1.HealthChecks.HealthStatusEnum.Healthy,
         HealthStatusEnum.Degraded    => Api.gRPC.V1.HealthChecks.HealthStatusEnum.Degraded,
         HealthStatusEnum.Unhealthy   => Api.gRPC.V1.HealthChecks.HealthStatusEnum.Unhealthy,
         _ => throw new ArgumentOutOfRangeException(nameof(status),
                                                    status,
                                                    null),
       };

  public static HealthStatusEnum ToInternalStatus(this Api.gRPC.V1.HealthChecks.HealthStatusEnum status)
    => status switch
       {
         Api.gRPC.V1.HealthChecks.HealthStatusEnum.Unspecified => HealthStatusEnum.Unspecified,
         Api.gRPC.V1.HealthChecks.HealthStatusEnum.Healthy     => HealthStatusEnum.Healthy,
         Api.gRPC.V1.HealthChecks.HealthStatusEnum.Degraded    => HealthStatusEnum.Degraded,
         Api.gRPC.V1.HealthChecks.HealthStatusEnum.Unhealthy   => HealthStatusEnum.Unhealthy,
         _ => throw new ArgumentOutOfRangeException(nameof(status),
                                                    status,
                                                    null),
       };
}
