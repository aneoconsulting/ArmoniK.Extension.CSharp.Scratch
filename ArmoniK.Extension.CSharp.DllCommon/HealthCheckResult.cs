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

namespace ArmoniK.Extension.CSharp.DllCommon;

/// <summary>
///   Detailed Result of a HealthCheck
/// </summary>
public record HealthCheckResult
{
  /// <summary>Indicates whether the HealthCheck passed or not.</summary>
  public bool IsHealthy { get; init; }

  /// <summary>Message describing the result of the healthCheck</summary>
  public string? Description { get; init; }

  /// <summary>Caught exception, if any.</summary>
  public Exception? Exception { get; init; }

  /// <summary>
  /// </summary>
  /// <returns></returns>
  public static HealthCheckResult Healthy()
    => new()
       {
         IsHealthy   = true,
         Description = "HealthCheck passed with success",
       };

  /// <summary>
  /// </summary>
  /// <param name="message"></param>
  /// <param name="ex"></param>
  /// <returns></returns>
  public static HealthCheckResult Unhealthy(string     message,
                                            Exception? ex = null)
    => new()
       {
         IsHealthy   = false,
         Description = message,
         Exception   = ex,
       };
}
