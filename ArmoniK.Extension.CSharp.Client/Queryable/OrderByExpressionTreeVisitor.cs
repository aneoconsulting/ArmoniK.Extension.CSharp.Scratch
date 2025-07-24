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

using System.Linq.Expressions;

namespace ArmoniK.Extension.CSharp.Client.Queryable;

/// <summary>
///   Class in charge of the construction of the protobuf instance that describe how the
///   results of a query should be sorted.
/// </summary>
/// <typeparam name="TSortField">The type of the protobuf instance</typeparam>
internal abstract class OrderByExpressionTreeVisitor<TSortField>
{
  /// <summary>
  ///   Visit the Expression Tree passed as parameter to the OrderBy() extension method.
  /// </summary>
  /// <param name="lambda">The Expression Tree of the lambda</param>
  /// <returns>The protobuf instance.</returns>
  public abstract TSortField Visit(LambdaExpression lambda);
}
