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

namespace ArmoniK.Extension.CSharp.Client.Common.Domain.Session;

/// <summary>
///   Represents session information, specifically encapsulating a session identifier.
/// </summary>
public record SessionInfo
{
  /// <summary>
  ///   Identifier of the session.
  /// </summary>
  public readonly string SessionId;

  /// <summary>
  ///   Initializes a new instance of the <see cref="SessionInfo" /> record with a specified session identifier.
  /// </summary>
  /// <param name="sessionId">The unique identifier for the session.</param>
  public SessionInfo(string sessionId)
    => SessionId = sessionId;
}
