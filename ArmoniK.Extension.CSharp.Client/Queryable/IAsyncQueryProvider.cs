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

using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;

namespace ArmoniK.Extension.CSharp.Client.Queryable;

/// <summary>
///   Interface for asynchronous query provider
/// </summary>
/// <typeparam name="T"></typeparam>
internal interface IAsyncQueryProvider<out T> : IQueryProvider
{
  /// <summary>
  ///   Execute a request with a specific filtering expression
  /// </summary>
  /// <param name="expression">The filtering expression tree</param>
  /// <param name="cancellationToken">The cancellation token</param>
  /// <returns>An asynchronous enumeration of instances compliant with the filter expression</returns>
  IAsyncEnumerable<T> ExecuteAsync(Expression        expression,
                                   CancellationToken cancellationToken);
}
