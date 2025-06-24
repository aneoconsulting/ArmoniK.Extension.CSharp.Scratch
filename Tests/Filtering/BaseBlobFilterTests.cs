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

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Blob;

using Google.Protobuf.WellKnownTypes;

namespace Tests.Filtering;

/// <summary>
///   Helper class to build protobuf structure for filtering
/// </summary>
public class BaseBlobFilterTests
{
  private static readonly Dictionary<string, ResultRawEnumField> memberName2EnumField_ = new()
                                                                                         {
                                                                                           {
                                                                                             nameof(BlobInfo.SessionId), ResultRawEnumField.SessionId
                                                                                           },
                                                                                           {
                                                                                             nameof(BlobInfo.BlobId), ResultRawEnumField.ResultId
                                                                                           },
                                                                                           {
                                                                                             nameof(BlobInfo.BlobName), ResultRawEnumField.Name
                                                                                           },
                                                                                           {
                                                                                             nameof(BlobState.CompletedAt), ResultRawEnumField.CompletedAt
                                                                                           },
                                                                                           {
                                                                                             nameof(BlobState.CreateAt), ResultRawEnumField.CreatedAt
                                                                                           },
                                                                                           {
                                                                                             nameof(BlobState.Status), ResultRawEnumField.Status
                                                                                           },
                                                                                         };

  private static readonly Dictionary<string, FilterStringOperator> op2EnumStringOp_ = new()
                                                                                      {
                                                                                        {
                                                                                          "==", FilterStringOperator.Equal
                                                                                        },
                                                                                        {
                                                                                          "!=", FilterStringOperator.NotEqual
                                                                                        },
                                                                                      };

  private static readonly Dictionary<string, FilterNumberOperator> op2EnumIntOp_ = new()
                                                                                   {
                                                                                     {
                                                                                       "==", FilterNumberOperator.Equal
                                                                                     },
                                                                                     {
                                                                                       "!=", FilterNumberOperator.NotEqual
                                                                                     },
                                                                                     {
                                                                                       "<", FilterNumberOperator.LessThan
                                                                                     },
                                                                                     {
                                                                                       "<=", FilterNumberOperator.LessThanOrEqual
                                                                                     },
                                                                                     {
                                                                                       ">", FilterNumberOperator.GreaterThan
                                                                                     },
                                                                                     {
                                                                                       ">=", FilterNumberOperator.GreaterThanOrEqual
                                                                                     },
                                                                                   };

  private static readonly Dictionary<string, FilterStatusOperator> op2EnumStatusOp_ = new()
                                                                                      {
                                                                                        {
                                                                                          "==", FilterStatusOperator.Equal
                                                                                        },
                                                                                        {
                                                                                          "!=", FilterStatusOperator.NotEqual
                                                                                        },
                                                                                      };

  private static readonly Dictionary<string, FilterDateOperator> op2EnumDateOp_ = new()
                                                                                  {
                                                                                    {
                                                                                      "==", FilterDateOperator.Equal
                                                                                    },
                                                                                    {
                                                                                      "!=", FilterDateOperator.NotEqual
                                                                                    },
                                                                                    {
                                                                                      "<", FilterDateOperator.Before
                                                                                    },
                                                                                    {
                                                                                      "<=", FilterDateOperator.BeforeOrEqual
                                                                                    },
                                                                                    {
                                                                                      ">", FilterDateOperator.After
                                                                                    },
                                                                                    {
                                                                                      ">=", FilterDateOperator.AfterOrEqual
                                                                                    },
                                                                                  };


  private ResultField BuildResultField(string fieldName)
    => new()
       {
         ResultRawField = new ResultRawField
                          {
                            Field = memberName2EnumField_[fieldName],
                          },
       };

  protected FilterField BuildFilterString(string fieldName,
                                          string op,
                                          string value)
    => new()
       {
         Field = BuildResultField(fieldName),
         FilterString = new FilterString
                        {
                          Operator = op2EnumStringOp_[op],
                          Value    = value,
                        },
       };

  protected FilterField BuildFilterInt(string fieldName,
                                       string op,
                                       int    value)
    => new()
       {
         Field = BuildResultField(fieldName),
         FilterNumber = new FilterNumber
                        {
                          Operator = op2EnumIntOp_[op],
                          Value    = value,
                        },
       };

  protected FilterField BuildFilterStatus(string     fieldName,
                                          string     op,
                                          BlobStatus value)
    => new()
       {
         Field = BuildResultField(fieldName),
         FilterStatus = new FilterStatus
                        {
                          Operator = op2EnumStatusOp_[op],
                          Value    = value.ToGrpcStatus(),
                        },
       };

  protected FilterField BuildFilterDateTime(string   fieldName,
                                            string   op,
                                            DateTime value)
    => new()
       {
         Field = BuildResultField(fieldName),
         FilterDate = new FilterDate
                      {
                        Operator = op2EnumDateOp_[op],
                        Value    = value.ToTimestamp(),
                      },
       };

  protected FiltersAnd BuildAnd(params FilterField[] filters)
  {
    var filterAnd = new FiltersAnd();
    foreach (var filter in filters)
    {
      filterAnd.And.Add(filter);
    }

    return filterAnd;
  }

  protected Filters BuildOr(params FiltersAnd[] filtersAnd)
  {
    var filterOr = new Filters();
    foreach (var filterAnd in filtersAnd)
    {
      filterOr.Or.Add(filterAnd);
    }

    return filterOr;
  }
}
