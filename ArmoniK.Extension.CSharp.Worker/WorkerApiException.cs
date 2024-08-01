// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2024. All rights reserved.
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

namespace ArmoniK.Extension.CSharp.Worker;

/// <summary>
///   General Worker API Exception
/// </summary>
public class WorkerApiException : Exception
{
  private readonly string message_ = "WorkerApi Exception during call function";

  /// <summary>
  ///   The ctor of WorkerApiException
  /// </summary>
  public WorkerApiException()
  {
  }

  /// <summary>
  ///   Th ctor to instantiate new thrown Exception with message
  /// </summary>
  /// <param name="message">The message that will be print in the exception</param>
  public WorkerApiException(string message)
    => message_ = message;

  /// <summary>
  ///   The ctor to instantiate new thrown Exception with previous exception
  /// </summary>
  /// <param name="e">The previous exception</param>
  public WorkerApiException(Exception e)
    : base(e.Message,
           e)
    => message_ = $"{message_} with InnerException {e.GetType()} message : {e.Message}";

  /// <summary>
  ///   The ctor with new message and the previous thrown exception
  /// </summary>
  /// <param name="message">The new message that will override the one from the previous exception</param>
  /// <param name="e">The previous exception</param>
  public WorkerApiException(string            message,
                            ArgumentException e)
    : base(message,
           e)
    => message_ = message;

  /// <summary>
  ///   The ctor with new message and the previous thrown exception
  /// </summary>
  /// <param name="message">The new message that will override the one from the previous exception</param>
  /// <param name="e">The previous exception</param>
  public WorkerApiException(string    message,
                            Exception e)
    : base(message,
           e)
    => message_ = message;

  /// <summary>
  ///   Overriding the Message property
  /// </summary>
  public override string Message
    => message_;
}
