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

namespace ArmoniK.Extension.CSharp.Worker.Interfaces.Common.Domain.Task;

/// <summary>
///   Represent an ArmoniK task result
/// </summary>
public class TaskResult
{
  private TaskResult()
  {
    IsSuccess    = true;
    ErrorMessage = string.Empty;
  }

  private TaskResult(string errorMessage)
  {
    IsSuccess    = false;
    ErrorMessage = errorMessage;
  }

  /// <summary>
  ///   Whether the ArmoniK task was successful
  /// </summary>
  public bool IsSuccess { get; init; }

  /// <summary>
  ///   Error message when IsSuccess == false, empty string otherwise
  /// </summary>
  public string ErrorMessage { get; init; }

  /// <summary>
  ///   Create a TaskResult that represent a success
  /// </summary>
  public static TaskResult Success
    => new();

  /// <summary>
  ///   Create a TaskResult that represent a failure
  /// </summary>
  /// <param name="errorMessage">The error message</param>
  /// <returns></returns>
  public static TaskResult Failure(string errorMessage)
    => new(errorMessage);
}
