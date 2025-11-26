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

using System.Text.Json.Serialization;

namespace ArmoniK.Extension.CSharp.Worker;

internal class Payload
{
  public Payload(IReadOnlyDictionary<string, string> inputs,
                 IReadOnlyDictionary<string, string> outputs)
  {
    Inputs  = inputs;
    Outputs = outputs;
  }

  [JsonPropertyName("inputs")]
  public IReadOnlyDictionary<string, string> Inputs { get; }

  [JsonPropertyName("outputs")]
  public IReadOnlyDictionary<string, string> Outputs { get; }
}
