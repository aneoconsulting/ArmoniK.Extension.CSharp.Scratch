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

using ArmoniK.Api.gRPC.V1.Agent;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Task;

namespace ArmoniK.Extension.CSharp.DllCommon.Common.Domain.Task;

public static class TaskInfosExt
{
  public static TaskInfos ToTaskInfos(this SubmitTasksResponse.Types.TaskInfo taskInfo,
                                      string                                  sessionId)
    => new()
       {
         TaskId           = taskInfo.TaskId,
         ExpectedOutputs  = taskInfo.ExpectedOutputIds,
         DataDependencies = taskInfo.DataDependencies,
         PayloadId        = taskInfo.PayloadId,
         SessionId        = sessionId,
       };
}
