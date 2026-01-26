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

namespace ArmoniK.Extensions.CSharp.Worker.Interfaces;

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
  ///   Creates a healthy result with the default success message.
  /// </summary>
  /// <returns>A new HealthCheckResult instance with IsHealthy set to true and a default success message.</returns>
  public static HealthCheckResult Healthy()
    => new()
       {
         IsHealthy   = true,
         Description = "HealthCheck passed with success",
       };

  /// <summary>
  ///   Creates a healthy result with a custom description.
  /// </summary>
  /// <param name="description">The custom description explaining the healthy status.</param>
  /// <returns>A new HealthCheckResult instance with IsHealthy set to true and the provided description.</returns>
  public static HealthCheckResult Healthy(string description)
    => new()
       {
         IsHealthy   = true,
         Description = description,
       };

  /// <summary>
  ///   Creates an unhealthy result with a custom message and optional exception.
  /// </summary>
  /// <param name="message">The error message describing why the health check failed.</param>
  /// <param name="ex">The exception that caused the health check failure, if any.</param>
  /// <returns>A new HealthCheckResult instance with IsHealthy set to false.</returns>
  public static HealthCheckResult Unhealthy(string     message,
                                            Exception? ex = null)
    => new()
       {
         IsHealthy   = false,
         Description = message,
         Exception   = ex,
       };
}
